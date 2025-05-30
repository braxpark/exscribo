#include "include/pgfe/data.hpp"
#include "include/pgfe/exceptions.hpp"
#include "include/pgfe/pgfe.hpp"
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_set>
#include <iterator>
#include "include/struct_mapping/struct_mapping.h"

namespace fs = std::filesystem;

namespace pgfe = dmitigr::pgfe;
using std::string, std::vector, std::unordered_map, std::unordered_set, std::queue;

const char DELIMITER = '\x1F';


// Overload the << operator for std::vector
template <typename T>
std::ostream& operator<<(std::ostream& os, const std::vector<T>& vec) {
    os << "[";
    for (size_t i = 0; i < vec.size(); ++i) {
        os << vec[i];
        if (i != vec.size() - 1) {
            os << ", "; // Add a comma and space between elements
        }
    }
    os << "]";
    return os;
}

struct CopyFromSupporters {
    std::unordered_map<std::string, std::string> tableToCol;
    std::unordered_map<std::string, std::string> tableToFilePath;
};

struct CopyFromDependent {
    std::string tableName;
    std::string filePath;
};

struct CopyFromDependents {
    std::unordered_map<std::string, std::string> tableToCol;
    std::unordered_map<std::string, std::string> tableToFilePath;
};

struct CopyFromSupporter {
    std::string tableName;
    std::string filePath;
};

enum PGDataType { NUMERIC, INTEGER, BIGINT, BOOLEAN, CHARACTERVARYING, TEXT, JSONB, TIMESTAMPNOTIMEZONE, DATE, OTHER };

struct DatabaseInfo {
    std::string host;
    int port;
    std::string name;
    std::string username;
    std::string password;
    bool sslEnabled;
};

struct DBConfig {
    DatabaseInfo source;
    DatabaseInfo destination;
};

struct ColInfo {
    bool isNullable;
    PGDataType dataType;
    int index;
};

struct RawColumn {
    std::string name;
    int index;
};


void parseFileIntoConfig(const std::string fileName, DBConfig& config) {
    // Assume file exists and is accessible
    std::ifstream ifs(fileName);
    assert(ifs.good());
    std::string content( (std::istreambuf_iterator<char>(ifs) ), (std::istreambuf_iterator<char>()));
    std::stringstream ssContent(content);

    struct_mapping::reg(&DBConfig::source, "source");
    struct_mapping::reg(&DBConfig::destination, "destination");
    struct_mapping::reg(&DatabaseInfo::host, "host");
    struct_mapping::reg(&DatabaseInfo::port, "port");
    struct_mapping::reg(&DatabaseInfo::name, "name");
    struct_mapping::reg(&DatabaseInfo::username, "username");
    struct_mapping::reg(&DatabaseInfo::password, "password");
    struct_mapping::reg(&DatabaseInfo::sslEnabled, "sslEnabled");

    struct_mapping::map_json_to_struct(config, ssContent);
}

std::string dependent_query = R"(SELECT
        tc.table_name as "tableName", 
        kcu.column_name, 
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name 
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema='public'
        AND ccu.table_name = )";

std::string supporter_query = R"(SELECT
        tc.table_name as "tableName", 
        kcu.column_name, 
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name 
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema='public'
        AND tc.table_name =)";

std::string valuesFromVector(const std::vector<std::string>& vec, const std::string& delimiter = ",") {
    std::stringstream s;
    copy(vec.begin(), vec.end(), std::ostream_iterator<std::string>(s, ","));
    return s.str().substr(0, s.str().length() - 1);
}

struct Table {
    string name;
    bool direct_descendant = false;
    unordered_map<string, string> supporters; // table -> col
    unordered_map<string, string> dependents; // table -> col
};

// Overload the << operator for Table
std::ostream& operator<<(std::ostream& os, const Table& table) {
    const std::string red = "\033[31m";
    const std::string reset = "\033[0m";
    os << red << "Table Name: " << reset << table.name << "\n";
    os << "Supporters:\n";
    for (const auto& supporter : table.supporters) {
        os << "  " << supporter.first << " -> " << supporter.second << "\n";
    }
    os << "Dependents:\n";
    for (const auto& dependent : table.dependents) {
        os << "  " << dependent.first << " -> " << dependent.second << "\n";
    }
    return os;
}

std::vector<std::string> topoSort(
    std::unordered_map<std::string, Table> tables,
    std::string root_table
) {
    std::vector<std::string> L;
    std::queue<Table*> S;
    for(auto& table : tables) {
        if(table.second.supporters.empty()) {
            S.push(&table.second);
        }
    }
    while(!S.empty()) {
        Table* curr = S.front();
        S.pop();
        L.push_back(curr->name);
        for(auto it = curr->dependents.begin(); it != curr->dependents.end(); ) {
            Table& m = tables[it->first];
            it = curr->dependents.erase(it);
            m.supporters.erase(curr->name);
            if(m.supporters.empty()) {
                S.push(&m);
            }
        }
    }
    assert(L.size() == tables.size());
    return L;
}

int main(int argc, char** argv)
{
    DBConfig config;
    parseFileIntoConfig(".env", config);
    std::cout << config.source.host << " - " << config.source.port << " - " << config.source.name << " - " << config.source.username << " - " << config.source.password << " - "  << config.source.sslEnabled << '\n';
    std::cout << "Params: \n";
    if(argc != 3) {
        std::cerr << "Must have 2 input args.\n";
        return -1;
    }
    string root_table = argv[1];
    string root_id = argv[2];
    for(int i = 0; i < argc; i++) {
        std::cout << argv[i] <<  '\n';
    }
    std::cout << '\n';
    auto beforeTime = std::chrono::steady_clock::now();
    try {
        pgfe::Connection conn{pgfe::Connection_options{}
        .set(pgfe::Communication_mode::net)
        .set_hostname(config.source.host)
        .set_port(config.source.port)
        .set_database(config.source.name)
        .set_username(config.source.username)
        .set_password(config.source.password)
        .set_ssl_enabled(config.source.sslEnabled)};
        conn.connect();

        unordered_map<string, unordered_map<string, unordered_map<string, string>>> fkey_map, inv_fkey_map;
        unordered_set<string> visited_tables;

        unordered_map<string, Table> table_info;

        queue<string> queue; 
        queue.push(root_table);
        table_info[root_table].name = root_table;
        table_info[root_table].direct_descendant = true; // root table is always a direct descendant
        while(!queue.empty()) {
            string curr_table = queue.front();
            queue.pop();
            if(visited_tables.count(curr_table)) {
                continue;
            } else {
                visited_tables.insert(curr_table);
            }
            table_info[curr_table].name = curr_table;
            string get_dependents_query = dependent_query + "'" + curr_table + "'";
            conn.execute([&](auto&& row)
              {
                    using dmitigr::pgfe::to;
                    string dependent_table_name = to<std::string>(row[0]), dependent_table_col_name = to<string>(row[1]); 
                    string supporter_table_name = to<std::string>(row[2]), supporter_table_col_name = to<string>(row[3]);

                    fkey_map[supporter_table_name][dependent_table_name][supporter_table_col_name] = dependent_table_col_name; //fkey[S][D][S.col] = D.col
                    inv_fkey_map[supporter_table_name][dependent_table_name][dependent_table_col_name] = supporter_table_col_name; // fkey[S][D][D.col] = S.col

                    table_info[dependent_table_name].supporters[supporter_table_name] = supporter_table_col_name;
                    table_info[supporter_table_name].dependents[dependent_table_name] = dependent_table_col_name;

                    table_info[dependent_table_name].direct_descendant = table_info[dependent_table_name].direct_descendant || table_info[supporter_table_name].direct_descendant;
                    queue.push(dependent_table_name);
              }, get_dependents_query);

            string get_supporters_query = supporter_query + "'" + curr_table + "'";
            conn.execute([&](auto&& row)
             {
                    using dmitigr::pgfe::to;
                    string dependent_table_name = to<std::string>(row[0]), dependent_table_col_name = to<string>(row[1]);
                    string supporter_table_name = to<std::string>(row[2]), supporter_table_col_name = to<string>(row[3]);

                    table_info[dependent_table_name].supporters[supporter_table_name] = supporter_table_col_name;
                    table_info[supporter_table_name].dependents[dependent_table_name] = dependent_table_col_name;

                    queue.push(supporter_table_name);
             }, get_supporters_query
            );
        }

        vector<string> sorted_table_names = topoSort(table_info, root_table);
        vector<Table> direct_descendants, non_direct_descendants;
        for(auto table : sorted_table_names) {
            if (table_info[table].direct_descendant) {
                direct_descendants.push_back(table_info[table]);
            } else {
                non_direct_descendants.push_back(table_info[table]);
            }
        }

        vector<Table> query_order = direct_descendants;
        query_order.insert(query_order.end(), non_direct_descendants.begin(), non_direct_descendants.end());
        vector<Table> insert_order = non_direct_descendants;
        insert_order.insert(insert_order.end(), direct_descendants.begin(), direct_descendants.end());

        fs::create_directory("query_order_results");

        auto psqlGetRootRow = [&config, root_table, root_id]() {
            std::ostringstream command;
            const auto& source = config.source; // Access the database source from the config
            std::string sslMode = source.sslEnabled ? "require" : "disable";

            command << "PGPASSWORD=" + source.password + " psql " +
            "-h " + source.host + " " +
            "-p " + std::to_string(source.port) + " " +
            "-d " + source.name + " " +
            "-U " + source.username + " ";
            if(sslMode == "require") {
                command << "sslmode=require";
            }
            command << "<<EOF\n";
            command << "\\copy (SELECT * FROM " << root_table << " WHERE id = '" << root_id << "') TO '" << fs::absolute(fs::path("query_order_results") / root_table).string() << "' WITH (DELIMITER '\x1F', HEADER);\n" << "EOF";
            return command.str();
        };

        auto psqlSourceTransaction = [&](const std::function<void(std::ostringstream&)>& func) {
            std::ostringstream command;
            const auto& source = config.source; // Access the database source from the config
            std::string sslMode = source.sslEnabled ? "require" : "disable";

             command << "PGPASSWORD=" + source.password + " psql " +
            "-h " + source.host + " " +
            "-p " + std::to_string(source.port) + " " +
            "-d " + source.name + " " +
            "-U " + source.username + " " + 
            "-v ON_ERROR_STOP=1";
            command << " <<EOF\n";


            command << "-- Step 1: Start a transaction\n";
            command << "BEGIN;\n\n";

            func(command);

            command << "-- Step 5: Commit the transaction\n";
            command << "COMMIT;\n";
            command << "EOF";

            return command.str();
        };

        unordered_set<string> loadedToTemp;

        auto isDirectDescendent = [&](const string& table) {
            return table_info[table].direct_descendant;
        };

        auto psqlCopyFrom = [&]() {
            return psqlSourceTransaction([&](std::ostringstream& command) {           
                for(auto table : query_order) {
                    if(table.name == root_table) continue;
                    command << "\\echo  Processing table: " << table.name << "\n";

                    if(table.direct_descendant || (!table.direct_descendant && table.supporters.size() > 0)) {
                        CopyFromSupporters supporters;
                        for(auto supp : table.supporters) {
                            string colName = fkey_map[supp.first][table.name][supp.second];
                            string sWithRespectToD = inv_fkey_map[supp.first][table.name][colName];
                            supporters.tableToCol[supp.first] = sWithRespectToD;
                            supporters.tableToFilePath[supp.first] = fs::absolute(fs::path("query_order_results") / supp.first).string();
                        }

                        CopyFromDependent dependent;
                        dependent.tableName = table.name;
                        dependent.filePath = fs::absolute(fs::path("query_order_results") / table.name).string();
                        std::ofstream outfile(dependent.filePath);
                        outfile.close();
                    
                        for(const auto& [table, col] : supporters.tableToCol) {
                            if(loadedToTemp.count(table)) continue;
                            loadedToTemp.insert(table);
                            command << "CREATE TEMP TABLE \"TEMP_" << table << "\" AS SELECT * FROM \"" << table << "\" WHERE 1=0;\n";

                            command << "\\copy \"TEMP_" << table << "\" FROM '" << supporters.tableToFilePath[table] << "' WITH (DELIMITER '\x1F', HEADER);\n";
                        }

                        command << "\\copy (SELECT DISTINCT \"" << dependent.tableName<< "\".* FROM \"" << dependent.tableName << "\" "; 
                        for(const auto& [sTable, sCol] : supporters.tableToCol) {
                            if(!table_info[sTable].direct_descendant && table.direct_descendant) continue;
                            string dCol = fkey_map[sTable][dependent.tableName][sCol];
                            command << "INNER JOIN \"TEMP_" << sTable << "\" ON \"" << dependent.tableName << "\".\"" << dCol << "\" = \"TEMP_" << sTable << "\".\"" << sCol << "\" ";
                        }

                        command << ") TO '" << dependent.filePath << "' WITH (DELIMITER '\x1F', HEADER);\n\n" ;
                    } else {
                        CopyFromDependents dependents;
                        for(auto dep : table.dependents) {
                            string colName = inv_fkey_map[table.name][dep.first][dep.second];
                            string dWithRespectToS = fkey_map[table.name][dep.first][colName];
                            dependents.tableToCol[dep.first] = dWithRespectToS;
                            dependents.tableToFilePath[dep.first] = fs::absolute(fs::path("query_order_results") / dep.first).string();
                        }
                        CopyFromSupporter supporter;
                        supporter.tableName = table.name;
                        supporter.filePath = fs::absolute(fs::path("query_order_results") / table.name).string();
                        std::ofstream outfile(supporter.filePath);
                        outfile.close();

                        for(const auto& [table, col] : dependents.tableToCol) {
                            if(loadedToTemp.count(table)) continue;
                            loadedToTemp.insert(table);
                            command << "CREATE TEMP TABLE \"TEMP_" << table << "\" AS SELECT * FROM \"" << table << "\" WHERE 1=0;\n";

                            command << "\\copy \"TEMP_" << table << "\" FROM '" << dependents.tableToFilePath[table] << "' WITH (DELIMITER '\x1F', HEADER);\n";
                        }
                        command << "\\copy (SELECT DISTINCT \"" << supporter.tableName << "\".* FROM \"" << supporter.tableName << "\" ";
                        for(const auto& [table, col] : dependents.tableToCol) {
                            string sCol = inv_fkey_map[supporter.tableName][table][col];
                            command << "LEFT JOIN \"TEMP_" << table << "\" ON \"" << supporter.tableName << "\".\"" << sCol << "\" = \"TEMP_" << table << "\".\"" << col << "\" ";
                        }
                        command << "WHERE ";
                        int index = 0;
                        for(const auto& [table, col] : dependents.tableToCol) {
                            if(index++ != 0) command << " OR ";
                            command << "\"TEMP_" << table << "\".\"" << col << "\" IS NOT NULL";
                        }
                        command << ") TO '" << supporter.filePath << "' WITH (DELIMITER '\x1F', HEADER);\n\n" ;
                    }
                }
            });
        };

        auto beforeCopyFromTime = std::chrono::steady_clock::now();
        std::ofstream outfile(fs::absolute(fs::path("query_order_results") / root_table));
        outfile.close();
        string psqlGetRootRowCommand = psqlGetRootRow();
        int command_res = system(psqlGetRootRowCommand.c_str());

        auto psqlCopyFromCommand = psqlCopyFrom();
        std::ofstream fullScriptOutFile("full_script.sql");
        fullScriptOutFile << "-- This script was generated by the program.\n";
        fullScriptOutFile << psqlGetRootRowCommand;
        fullScriptOutFile << psqlCopyFromCommand;
        fullScriptOutFile.close();
        command_res = system(psqlCopyFromCommand.c_str());
        assert(!command_res);

        auto psqlCopyTo = [&config](string tableName, string filePath) {
            // Start building the psql command
            std::ostringstream command;
            const auto& destination = config.destination; 

            command << "PGPASSWORD=" + destination.password + " psql " +
            "-h " + destination.host + " " +
            "-p " + std::to_string(destination.port) + " " +
            "-d " + destination.name + " " +
            "-U " + destination.username + " ";
            command << " <<EOF\n";

            command << "-- Step 1: Start a transaction\n";
            command << "BEGIN;\n\n";
            command << "\\echo  Processing table: " << tableName << "\n";
            command << "\\copy " << tableName << " FROM '" << filePath << "' WITH (DELIMITER '\x1F', HEADER);\n";
            command << "COMMIT;\n";
            command << "EOF";

            return command.str();
        };

        for(auto table : insert_order) {
            string psqlCopyToCommand = psqlCopyTo(table.name, fs::absolute(fs::path("query_order_results") / table.name).string());
            int command_res = system(psqlCopyToCommand.c_str());
            assert(!command_res);
        }

        std::chrono::time_point afterTime = std::chrono::steady_clock::now();
        std::chrono::duration<float> elapsedTime = afterTime - beforeTime;
        std::chrono::duration<float> elapsedTimeCopyFrom = afterTime - beforeCopyFromTime;
        std::cout << "Program ran in: " << elapsedTime.count() << '\n';
        std::cout << "CopyFromSource ran in: " << elapsedTimeCopyFrom.count() << '\n';
        std::cout << fs::current_path() << '\n';
        //std::cout << "Cleaning query_order_results directory...\n";
        //fs::remove_all("query_order_results");
        conn.disconnect();

    } catch (const pgfe::Server_exception& e) {
        std::cout << e.error().detail() << '\n';
        assert(e.error().condition() == pgfe::Server_errc::c42_syntax_error);
        std::printf("Error %s is handled as expected.\n", e.error().sqlstate());
} catch (const std::exception& e) {
    std::printf("Oops: %s\n", e.what());
    return 1;

    }
}
