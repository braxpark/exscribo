#include "include/pgfe/data.hpp"
#include "include/pgfe/exceptions.hpp"
#include "include/pgfe/pgfe.hpp"
#include <cassert>
#include <chrono>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_set>
#include <map>
#include "include/struct_mapping/struct_mapping.h"

namespace fs = std::filesystem;

namespace pgfe = dmitigr::pgfe;

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

const char DELIMITER = 29;

const std::string hexDelimiter = "1D";

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

std::unordered_map<std::string, int8_t> columnIndexesFromRow(std::unordered_set<std::string> columns, dmitigr::pgfe::Row& row) {
    std::unordered_map<std::string, int8_t> colIndexes;
    size_t fieldCount = row.field_count();
    for(auto& col : columns) {
        size_t fieldIndex = row.field_index(col);
        assert(fieldIndex != fieldCount);
        colIndexes[col] = fieldIndex;
    }
    return colIndexes;
}


std::unordered_map<std::string, int8_t> columnIndexesFromHeader(std::unordered_set<std::string> columns, const std::string& header) {
    std::stringstream headerStream(header);
    std::string col;
    std::unordered_map<std::string, int8_t> colIndexes;
    size_t colIndex = 0;
    while(std::getline(headerStream, col, DELIMITER)) {
        for(auto& colName : columns){
            if(col == colName) {
                colIndexes[col] = colIndex;
                break;
            }
        }
        colIndex++;
    }
    return colIndexes;
}

std::string getChildrenQuery = R"(SELECT
        tc.table_schema, 
        tc.constraint_name, 
        tc.table_name as "tableName", 
        kcu.column_name, 
        ccu.table_schema AS foreign_table_schema,
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
        AND ccu.table_name =')";

std::string getSupportersQuery = R"(SELECT
        tc.table_schema, 
        tc.constraint_name, 
        tc.table_name as "tableName", 
        kcu.column_name, 
        ccu.table_schema AS foreign_table_schema,
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
        AND tc.table_name =')";

std::string getTableFieldsAndDataTypes(const std::string& tableName) {
    return R"(
        SELECT column_name, is_nullable, data_type
        FROM information_schema.columns WHERE table_name = ')" + tableName + "'";
}

std::string valuesFromVector(const std::vector<std::string>& vec, const std::string& delimiter = ",") {
    std::stringstream s;
    copy(vec.begin(), vec.end(), std::ostream_iterator<std::string>(s, ","));
    return s.str().substr(0, s.str().length() - 1);
}

std::string getRowsByFKEYQuery(const std::string& tableName, const std::string& colName, const std::string& colValue, bool isString = false, const std::string& where = "") {
    const std::string value = isString ? "'" + colValue + "'" : colValue;
    return (R"(
        SELECT
            *
        FROM )" + tableName +
    R"( WHERE )" + colName +
    R"( = )" + value + where );
}

std::string getSupporterQuery(const std::string& tableName) {
    return getSupportersQuery + tableName + "'";
}

struct sortDepListOnDependencySize {
    inline bool operator()(std::pair<std::string, std::unordered_set<std::string>>& a, std::pair<std::string, std::unordered_set<std::string>>& b) {
        return a.second.size() < b.second.size();
    }
};

std::string getForeignKeyQuery(const std::string& tableName) {
    return getChildrenQuery + tableName + "'";
}

PGDataType getPGDataType(const std::string& dataType) {
    if(dataType == "integer") return PGDataType::INTEGER;
    else if(dataType == "bigint") return PGDataType::BIGINT;
    else if(dataType == "numeric") return PGDataType::NUMERIC;
    else if(dataType == "boolean") return PGDataType::BOOLEAN;
    else if(dataType == "character varying") return PGDataType::CHARACTERVARYING;
    else if(dataType == "text") return PGDataType::TEXT;
    else if(dataType == "jsonb") return PGDataType::JSONB;
    else if(dataType == "timestamp without time zone") return PGDataType::TIMESTAMPNOTIMEZONE;
    else if(dataType == "date") return PGDataType::DATE;
    return PGDataType::OTHER;
}

// is there a better way of pattern matching?
bool pgDataTypeNeedsEnclosedQuotes(const PGDataType& dataType) {
    std::vector<PGDataType> encloseds = { PGDataType::CHARACTERVARYING, PGDataType::TEXT, PGDataType::JSONB, PGDataType::TIMESTAMPNOTIMEZONE, PGDataType::DATE, PGDataType::OTHER };
    for(auto& dt : encloseds) {
        if(dt == dataType) return true;
    }
    return false;
}


void parseRawRowData(std::ifstream& infile, std::ofstream& outfile, std::vector<RawColumn> cols, int64_t& totalRows) {
    if(cols.size() == 0) return;
    std::string str;
    bool firstLine = true;
    std::string header;
    int lineNumber = 0;
    while(std::getline(infile, str)) {
        if(!lineNumber) {
            lineNumber++; continue;
        }
        lineNumber++;
        totalRows++;
        std::vector<std::string> values;
        std::stringstream lineStream(str);
        std::string cell;
        while(std::getline(lineStream, cell, DELIMITER)) {
            values.push_back(cell);
        }
        std::string parsedRow = "";
        for(size_t i = 0; i < cols.size(); i++) {
            auto col = cols[i];
            if(i > 0) {
                parsedRow += DELIMITER;
            }
            parsedRow += values[col.index];
        }
        if(firstLine) {
            firstLine = false;
            std::string colNames = "";
            for(size_t i = 0; i < cols.size(); i++) {
                auto col = cols[i];
                if(i > 0) {
                    colNames += DELIMITER;
                }
                colNames += col.name;
            }
            outfile << colNames << std::endl;
        }
        outfile << parsedRow << std::endl;
    }
}

int main(int argc, char** argv)
{
    DBConfig config;
    parseFileIntoConfig(".env", config);
    std::cout << config.source.host << " - " << config.source.port << " - " << config.source.name << " - " << config.source.username << " - " << config.source.password << " - "  << config.source.sslEnabled << '\n';
    std::cout << "Params: \n";
    for(int i = 0; i < argc; i++) {
        std::cout << argv[i] <<  '\n';
    }
    std::cout << '\n';
    auto beforeTime = std::chrono::steady_clock::now();
    std::string retailerQuery = getForeignKeyQuery(argv[1]);
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
        
        /*
        pgfe::Connection local{pgfe::Connection_options{}
            .set(pgfe::Communication_mode::net)
            .set_hostname("localhost")
            .set_port(5433)
            .set_database("deductions_app_development")
            .set_username("postgres")
            .set_password("postgres")
            //.set_ssl_enabled(true)
        };
        //local.connect();
        */
        
        std::unordered_set<std::string> seen;
        std::unordered_map<std::string, bool> directDescendants;
        std::unordered_map<std::string, bool> outsideTables;
        std::map<std::string, std::unordered_set<std::string>> deps;
        std::map<std::string, std::unordered_set<std::string>> inv;
        std::unordered_map<std::string, std::unordered_set<std::string>> locks;
        std::unordered_map<std::string, std::unordered_map<std::string, std::string>> fkeys;
        std::unordered_map<std::string, std::unordered_map<std::string, std::string>> invFkeys;
        std::unordered_map<std::string, std::unordered_map<std::string, std::string>> fkeyCols;  // fkeyCols[table_foo][column_name] = foreign_column_name
        std::unordered_map<std::string, std::unordered_set<std::string>> tableFkeyNeeds;
        std::unordered_map<std::string, std::unordered_set<std::string>> outsideTableFkeyNeeds;
        std::unordered_map<std::string, std::unordered_set<std::string>> invTableFkeyNeeds;
        std::unordered_map<std::string, std::unordered_map<std::string, ColInfo>> tableCols;
        std::unordered_map<std::string, std::unordered_map<std::string, std::string>> tableDependencyFKeys;
        std::unordered_map<std::string, std::unordered_map<std::string, std::vector<std::string>>> tableColValues;
        std::vector<std::string> tableOrder = {argv[1]};
        std::queue<std::string> q;
        q.push(std::string(argv[1]));
        seen.insert(std::string(argv[1]));
        directDescendants[std::string(argv[1])] = true;
        while(!q.empty()) {
            std::string currentTable = q.front();
            q.pop();
            std::string dependants = getForeignKeyQuery(currentTable);
            std::string supporters = getSupporterQuery(currentTable);
            conn.execute([&](auto&& r)
            {
                using dmitigr::pgfe::to;
                std::vector<std::string> cols;
                auto dependentTable = to<std::string>(r["tableName"]);
                auto colName = to<std::string>(r["column_name"]);
                auto foreignColName = to<std::string>(r["foreign_column_name"]);
                fkeyCols[currentTable][colName] = foreignColName; // colName == dependent table col, foreignColName is current Table colName
                tableFkeyNeeds[currentTable].insert(foreignColName); // table A == supporting table, foreignColName is column within A needed to foreign key onto dependant tables of A
                invTableFkeyNeeds[dependentTable].insert(colName);
                fkeys[dependentTable][currentTable] = colName; // supporter's col name
                invFkeys[currentTable][dependentTable] = colName;
                tableDependencyFKeys[dependentTable][currentTable] = colName;
                if(seen.count(dependentTable) == 0) {
                    q.push(dependentTable);
                }
                seen.insert(dependentTable);
                // deps[B] = B depends on [..A]
                // inv[A] = A supports [..B]
                
                if(directDescendants.count(currentTable) > 0 && directDescendants[currentTable]) {
                    directDescendants[dependentTable] = true;
                } else if(directDescendants.count(dependentTable) == 0) {
                    outsideTables[dependentTable] = true;
                }

                deps[dependentTable].insert(currentTable);
                inv[currentTable].insert(dependentTable);
                tableOrder.push_back(dependentTable);
            },
            dependants);
            conn.execute([&](auto&& r){
                using dmitigr::pgfe::to;
                auto foreignTableName = to<std::string>(r["foreign_table_name"]);
                auto foreignKeyCol = to<std::string>(r["foreign_column_name"]);
                auto tableName = to<std::string>(r["tableName"]);
                auto colName = to<std::string>(r["column_name"]);
                if(seen.count(foreignTableName) == 0) {
                    q.push(foreignTableName);
                }
                if(directDescendants.count(foreignTableName) == 0) {
                    outsideTables[foreignTableName] = true;
                }
                seen.insert(foreignTableName);
                deps[currentTable].insert(foreignTableName);
                inv[foreignTableName].insert(currentTable);
                outsideTableFkeyNeeds[tableName].insert(colName);
                tableFkeyNeeds[tableName].insert(colName); // table A == supporting table, foreignColName is column within A needed to foreign key onto dependant tables of A
                fkeyCols[currentTable][colName] = foreignKeyCol;
                tableDependencyFKeys[tableName][foreignTableName] = colName;
                fkeys[tableName][foreignTableName] = colName; // supporter's col name
                invFkeys[tableName][foreignTableName] = foreignKeyCol;
            }, supporters);
            std::string colQuery = getTableFieldsAndDataTypes(currentTable);
            conn.execute([&](auto&& r) {
                using dmitigr::pgfe::to;
                std::string colName = to<std::string>(r["column_name"]);
                std::string isNullable = to<std::string>(r["is_nullable"]);
                std::string dataType = to<std::string>(r["data_type"]);
                tableCols[currentTable][colName].isNullable = isNullable == "YES" ? true : false;
                tableCols[currentTable][colName].dataType = getPGDataType(dataType);
            }, colQuery);
        }

        auto depCopy = deps;
        std::vector<std::string> order;
        q.push(argv[1]);

        auto hasIncomingEdges =[&](const std::string& table) {
            return inv.count(table) > 0;
        };

        // kahn's algorithm
        std::vector<std::string> L;
        std::queue<std::string> S;

        std::vector<std::string> others; // NOT direct descendants;
        //
        for(auto& table : outsideTables) {
            others.push_back(table.first);
        }

        int outsideTablesSize = outsideTables.size(), directDescendantsSize = directDescendants.size();
        assert(outsideTables.size() + directDescendants.size() == seen.size());

        for(auto& table : seen) {
            if(deps[table].size() == 0) {
                S.push(table);
            }
        }

        auto invCopyS = inv;
        auto depsCopyS = deps;

        // https://en.wikipedia.org/wiki/Topological_sorting#:~:text=.-,Kahn%27s%20algorithm,-%5Bedit%5D
        while(!S.empty()) {
            std::string currentTable = S.front();
            S.pop();
            L.push_back(currentTable);
            std::vector<std::string> toErase;
            for(std::string m : invCopyS[currentTable]) {
                toErase.push_back(m);
                if(depsCopyS[m].size() == 1) {
                    S.push(m);
                }  
            }
            for(auto& erase : toErase) {
                invCopyS[currentTable].erase(erase);
                depsCopyS[erase].erase(currentTable);
            }
        }
        // kahns on the non direct descendants  -> inverted on the dep/supporter relationship
        std::vector<std::string> othersL;
        std::queue<std::string> O;
        auto invCopyO = inv;
        auto depsCopyO = deps;
        for(auto& [table, isOutside] : outsideTables) {
            bool flag = false;
            for(auto& t : inv[table]) {
                bool othersContains = false;
                for(auto& [o, isOutside] : outsideTables) {
                    if(o != table && t == o) {
                        othersContains = true;
                        break;
                    }
                }
                if(othersContains) {
                    flag = true;
                    break;
                }
            }
            if(!flag) {
                O.push(table);
            }
        }

        while(!O.empty()) {
            std::string currentTable = O.front();
            O.pop();
            othersL.push_back(currentTable);
            std::vector<std::string> toErase;
            for(std::string m : invCopyS[currentTable]) {
                toErase.push_back(m);
                if(depsCopyS[m].size() == 1) {
                    S.push(m);
                }  
            }
            for(auto& erase : toErase) {
                invCopyS[currentTable].erase(erase);
                depsCopyS[erase].erase(currentTable);
            }
        }

        conn.execute([&](auto&& r)
            {
                using dmitigr::pgfe::to;
                auto id = to<std::string>(r["id"]);
                tableColValues[std::string(argv[1])]["id"].push_back(id);
                for(auto col : r) {
                    //std::cout << to<std::string>(col) << '\n';
                }
            },
            ("select * from " + std::string(argv[1]) + " where id = " + std::string(argv[2])));

        int64_t totalRows = 0;

        std::vector<std::string> descendantSet = {};
        for(auto& l : L) {
            if(directDescendants.count(l) > 0 && directDescendants[l]) {
                descendantSet.push_back(l);
            }
        }
        for(auto desc : descendantSet) {
            std::cout << desc << '\n';
        }
        return 0;
        /*
        auto getValuesForTable = [&](const std::string& tableName, const std::string& dependantTable, std::string option = "desc"){
            // in /data_search
            std::string fkeysVal = option == "desc" ? fkeys[tableName][dependantTable] : fkeys[dependantTable][tableName];
            std::string fkeyCol = option == "desc" ? fkeyCols[dependantTable][fkeys[tableName][dependantTable]] : fkeysVal;
            // need fkey from dependant table
            // eventually could probably use better file handling but for now this is probably fine...
            std::ifstream infile("../../" + dependantTable + "/data_search/" + dependantTable + "_parsed.csv");
            assert(infile.is_open());
            std::string line;
            size_t lineNumber = 0;
            int fkeyColIndex = INT_MIN;
            std::vector<std::string> values;
            while(std::getline(infile, line)) {
                std::vector<std::string> lineValues;
                std::stringstream lineStream(line);
                std::string cell;
                while(std::getline(lineStream, cell, DELIMITER)) {
                        lineValues.push_back(cell);
                }
                if(lineNumber == 0) {
                    for(size_t idx = 0; idx < lineValues.size(); idx++) {
                        if(lineValues[idx] == fkeyCol) {
                            fkeyColIndex = idx; break;
                        }
                    }
                    assert(fkeyColIndex != INT_MIN);
                } else {
                    values.push_back(lineValues[fkeyColIndex]);
                }
                lineNumber++;
            }
            infile.close();
            return values;
        };
        
        auto dataSearchDescendantWhere = [&](const std::string& tableName){
            if(tableName == std::string(argv[1])) {
                return "WHERE id = " + std::string(argv[2]);
            }
            std::string where = "WHERE 1 = 2";
            bool flag = false;
            for(auto& dependantTable : deps[tableName]) {
                if(directDescendants.count(dependantTable) == 0 || !directDescendants[dependantTable]) continue;
                std::string foreignKey = tableDependencyFKeys[tableName][dependantTable];
                std::vector<std::string> values = getValuesForTable(tableName, dependantTable);
                if(values.size()) {
                    flag = true;
                    std::string foreignWrappedTableName = "\"" + foreignKey + "\"";
                    where += (" OR " + foreignWrappedTableName + " IN " + "(" + valuesFromVector(values) + ")");
                }
            }
            return where;
        };

        auto dataSearchNonDescendantWhere = [&](const std::string& tableName){
            std::string where = "WHERE 1 = 2";
            bool flag = false;
            for(auto& dependantTable : inv[tableName]) {
                std::string foreignKey = invFkeys[dependantTable][tableName];
                std::vector<std::string> values = getValuesForTable(tableName, dependantTable, "nondesc");
                std::vector<std::string> noNulls;
                for(auto& val : values) {
                    if(val != "\\N") {
                        noNulls.push_back(val);
                    }
                }
                if(noNulls.size()) {
                    flag = true;
                    std::string foreignWrappedTableName = "\"" + foreignKey + "\"";
                    where += (" OR " + foreignWrappedTableName + " IN " + "(" + valuesFromVector(noNulls) + ")");
                }  
            }
            return where;
        };

        auto dataSearchTable = [&](const std::string& tableName, std::string option = "desc"){
            std::string query = "SELECT * FROM " + tableName + " " + (option == "desc" ? dataSearchDescendantWhere(tableName) : dataSearchNonDescendantWhere(tableName));
            return query;
        };  
        fs::path dataDirectory = "data";
        fs::create_directory(dataDirectory);
        fs::current_path(dataDirectory);

        auto psqlCommand = [&](const std::string& tableName, const std::string& pathToTableData) {
            std::string command = "PGPASSWORD=" + config.source.password;
            command += " psql";
            command += (" --host=" + config.source.host);
            command += (" --port=" + std::to_string(config.source.port));
            command += (" --username=" + config.source.username);
            command += (" --dbname=" + config.source.name);
            command += R"( -c "\copy )" + tableName + R"( FROM ')" + pathToTableData + R"(' WITH DELIMITER )" + hexDelimiter + R"(' HEADER")";
            return command;
        };

        auto psqlCopyToCommand = [&](const std::string& tableName, const std::string& query) {
            // in ~data
            std::string pathToTableData = fs::current_path().string() + "/" + tableName + "/data_search/" + tableName + ".csv";
            std::string pathToCopyTo = fs::current_path().string() + "/" + tableName + "_bulk_copy.csv";
            std::ofstream outfile(pathToCopyTo);
            outfile.close();
            std::stringstream ss;
            ss << "E'\\x" << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(static_cast<unsigned char>(DELIMITER));
            std::string hexDelimiter = ss.str();
            // this is the write to db command:w
            std::string command = "PGPASSWORD=" + config.source.password;
            command += " psql";
            command += (" --host=localhost");
            command += (" --port=5443");
            command += (" --username=postgres");
            command += (" --dbname=postgres");
            command += R"( -c "\copy ()" + query + ") TO '" + pathToCopyTo + "'" +  R"( DELIMITER )" + hexDelimiter + R"(' HEADER")";
            return command;
        };

        auto doTableDataSearch = [&](std::vector<std::string> tableList, std::unordered_map<std::string, std::unordered_set<std::string>> neededFKeys, std::string option = "desc") {
            for(auto& tableName : tableList) {
                fs::create_directory(fs::path(tableName));
                fs::path tableDir = tableName;
                fs::current_path(tableDir);
                fs::path dataSearchPath = "data_search";
                fs::create_directory(dataSearchPath);
                fs::current_path(dataSearchPath);
                std::string query = dataSearchTable(tableName, option);
                std::ofstream fout(tableName + ".csv");
                std::ofstream parsed(tableName + "_parsed.csv");
                bool firstRow = true;
                std::unordered_map<std::string, int8_t> colIndexes;
                std::unordered_set<std::string> neededFkeyColumns = neededFKeys[tableName];
                size_t numberOfRows = 0;
                std::string cmd = psqlCopyToCommand(tableName, query);
                int result = system(cmd.c_str());
                if(!result) {
                    std::cout << "Copied " << tableName << " from source successful!\n";
                } 
                std::ifstream infile(tableName + "_bulk_copy.csv");
                std::string header;
                std::getline(infile, header);
                std::string dataRowOne;
                colIndexes = columnIndexesFromHeader(neededFkeyColumns, header);
                fout.close();
                if(std::getline(infile, dataRowOne)) {
                    std::ifstream rawInfile(tableName + "_bulk_copy.csv");
                    std::vector<RawColumn> cols;
                    for(auto& fkey : neededFkeyColumns) {
                        RawColumn col;
                        col.name = fkey;
                        assert(colIndexes.count(fkey) > 0);
                        col.index = colIndexes[fkey];
                        cols.push_back(col);
                    }
                    if(cols.size()) { // only create _parsed file if there are columns needed
                        parseRawRowData(rawInfile, parsed, cols, totalRows);
                    }
                }
                fs::current_path(fs::current_path().parent_path().parent_path());  // /data
            }
        };

        doTableDataSearch(descendantSet, tableFkeyNeeds);
        std::vector<std::string> outsideSet;
        for(auto& [t, ot] : outsideTables) outsideSet.push_back(t);
        othersL = outsideSet;
        doTableDataSearch(othersL, outsideTableFkeyNeeds, "nonDesc");

        std::ofstream outfile("graph-info.txt");
        // now create and run copy from queries to load the data search results into destination db
        std::string tab = "     ";
        int numRows=0, numSeenRows = 0;
        for(auto& t : directDescendants) {
            numRows++;
            outfile << t.first <<'\n';
        }
        for(auto& t : outsideTables) {
            numRows++;
            outfile << t.first << '\n';
        }
        outfile << numRows << std::endl;
        outfile << "<------------------------------>\n";
        for(auto& s : seen) {
            numSeenRows++;
            outfile << s << '\n';
        }
        outfile << numSeenRows << std::endl;

        auto psqlCopyFromCommand = [&](const std::string& tableName) {
            std::string pathToTableData = fs::current_path().string() + "/" + tableName + "/data_search/" + tableName + "_bulk_copy.csv";
            std::stringstream ss;
            ss << "E'\\x" << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(static_cast<unsigned char>(DELIMITER));
            std::string hexDelimiter = ss.str();
            std::string command = psqlCommand(tableName, pathToTableData);
            command += R"( -c "\copy )" + tableName + R"( FROM ')" + pathToTableData + R"(' WITH DELIMITER )" + hexDelimiter + R"(' HEADER")";
            return command;
        };

        outfile << "<------------->\n";
        for(auto& t : L) {
            std::string command = psqlCopyFromCommand(t);
            int commandSysResult = system(command.c_str());
            if(commandSysResult) {
                std::cout << "Copied " << t << " successfully.\n";
            }
            outfile << command  << std::endl;
        }

        outfile.close();
        // still need to do cleanup of locally written files
        */

        std::chrono::time_point afterTime = std::chrono::steady_clock::now();
        std::chrono::duration<float> elapsedTime = afterTime - beforeTime;
        std::cout << "Program ran in: " << elapsedTime.count() << '\n';
        std::cout << "Total Number of Rows: " << totalRows << '\n';
        std::cout << fs::current_path() << '\n';

    } catch (const pgfe::Server_exception& e) {
        std::cout << e.error().detail() << '\n';
        assert(e.error().condition() == pgfe::Server_errc::c42_syntax_error);
        std::printf("Error %s is handled as expected.\n", e.error().sqlstate());
} catch (const std::exception& e) {
    std::printf("Oops: %s\n", e.what());
    return 1;

    }
}
