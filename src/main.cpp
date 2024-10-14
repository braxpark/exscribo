#include "include/src/pgfe/data.hpp"
#include "include/src/pgfe/exceptions.hpp"
#include "include/src/pgfe/pgfe.hpp"
#include <cassert>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <unordered_set>
#include <vector>
#include <map>
#include "struct_mapping/struct_mapping.h"

namespace fs = std::filesystem;

namespace pgfe = dmitigr::pgfe;

struct DatabaseInfo {
    std::string host;
    std::string dbName;
    std::string username;
    std::string password;
};

enum PGDataType { NUMERIC, INTEGER, BIGINT, BOOLEAN, CHARACTERVARYING, TEXT, JSONB, TIMESTAMPNOTIMEZONE, DATE, OTHER };

void parseFileIntoConfig(const std::string fileName, DatabaseInfo& config) {
    // Assume file exists and is accessible
    std::ifstream ifs(fileName);
    assert(ifs.good());
    std::string content( (std::istreambuf_iterator<char>(ifs) ), (std::istreambuf_iterator<char>()));
    std::stringstream ssContent(content);
    struct_mapping::reg(&DatabaseInfo::host, "host");
    struct_mapping::reg(&DatabaseInfo::dbName, "dbName");
    struct_mapping::reg(&DatabaseInfo::username, "username");
    struct_mapping::reg(&DatabaseInfo::password, "password");
    struct_mapping::map_json_to_struct(config, ssContent);
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

struct ColInfo {
    bool isNullable;
    PGDataType dataType;
    int index;
};

int main(int argc, char** argv)
{
    DatabaseInfo config;
    parseFileIntoConfig("dataSource.json", config);
    std::cout << config.host << " - " << config.dbName << " - " << config.username << " - " << config.password << '\n';
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
            .set_hostname(config.host)
            .set_database(config.dbName)
            .set_username(config.username)
            .set_password(config.password)
            .set_ssl_enabled(true)
        };
        conn.connect();

        pgfe::Connection local{pgfe::Connection_options{}
            .set(pgfe::Communication_mode::net)
            .set_hostname("localhost")
            .set_database("deductions_app_development")
            .set_username("postgres")
            .set_password("postgres")
            //.set_ssl_enabled(true)
        };

        local.connect();
        
        std::unordered_set<std::string> seen;
        std::unordered_map<std::string, bool> directDescendants;
        std::map<std::string, std::unordered_set<std::string>> deps;
        std::map<std::string, std::unordered_set<std::string>> inv;
        std::unordered_map<std::string, std::unordered_set<std::string>> locks;
        std::unordered_map<std::string, std::unordered_map<std::string, std::string>> fkeys;
        std::unordered_map<std::string, std::unordered_map<std::string, std::string>> invFkeys;
        std::unordered_map<std::string, std::unordered_map<std::string, std::string>> fkeyCols;  // fkeyCols[table_foo][column_name] = foreign_column_name
        std::unordered_map<std::string, std::unordered_set<std::string>> tableFkeyNeeds;
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
                fkeyCols[currentTable][colName] = foreignColName;
                tableFkeyNeeds[currentTable].insert(foreignColName);
                invTableFkeyNeeds[dependentTable].insert(colName);
                fkeys[dependentTable][currentTable] = colName; // supporter's col name
                invFkeys[currentTable][dependentTable] = colName;
    

                tableDependencyFKeys[dependentTable][currentTable] = colName;


                if(seen.count(dependentTable) == 0) {
                    seen.insert(dependentTable);
                    q.push(dependentTable);
                }

                // deps[B] = B depends on [..A]
                // inv[A] = A supports [..B]
                
                bool isDescendant = directDescendants[currentTable];
                directDescendants[dependentTable] = (directDescendants.count(currentTable) > 0 && directDescendants[currentTable]) || directDescendants[dependentTable];
                if(dependentTable == "deductions") {
                         std::cout << "desc count and is desc: " << directDescendants.count(currentTable) <<  " -- " << (directDescendants[currentTable] ? "true" : "false") << " -- " << currentTable << '\n';
                }
                deps[dependentTable].insert(currentTable);
                inv[currentTable].insert(dependentTable);
                tableOrder.push_back(dependentTable);
            },
            dependants);
            conn.execute([&](auto&& r){
                using dmitigr::pgfe::to;
                auto tableName = to<std::string>(r["foreign_table_name"]);
                //std::cout << currentTable << " depends on: " << tableName << '\n';
                if(seen.count(tableName) == 0) {
                    seen.insert(tableName);
                    q.push(tableName);
                }
                deps[currentTable].insert(tableName);
                inv[tableName].insert(currentTable);
                
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
        
        /*
        for(auto& d : deps) {
            std::cout << d.first << " depends on: ";
            for(auto& di: d.second) {
                std::cout << di << " | ";
            }
            std::cout << '\n';
        }
        */

        // kahn's algorithm
        std::vector<std::string> L;
        std::queue<std::string> S;

        std::vector<std::string> others; // NOT direct descendants;
        //
        for(auto& table : seen) {
            if(directDescendants.count(table) == 0 || !directDescendants[table]) {
                others.push_back(table);
            }
        }

        for(auto& table : seen) {
            if(deps[table].size() == 0) {
               // std::cout << "Adding: " << table << '\n';
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
        for(auto& table : others) {
            bool flag = false;
            for(auto& t : inv[table]) {
                bool othersContains = false;
                for(auto o : others) {
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
                /*
                for(auto& col : tableFkeyNeeds[argv[1]]) {
                    auto val = to<std::string>(r[col]);
                    tableColValues[std::string(argv[1])][col].push_back(val);
                }
                */
            },
            ("select * from " + std::string(argv[1]) + " where id = " + std::string(argv[2])));

        std::cout << "supplier stuff: " << tableColValues[std::string(argv[1])]["id"][0] << '\n';
        // maybe need to rethink this
        auto whereCondition = [&](std::string tableName) {
            std::string whereCondition = "";
            std::cout << tableName << '\n';
            bool first = true;
            for(auto& dep : depCopy[tableName]) {
                std::string tableCol = fkeys[tableName][dep];
                std::string mappedTableCol = fkeyCols[dep][tableCol];
                std::vector<std::string> values = tableColValues[dep][fkeyCols[dep][fkeys[tableName][dep]]];
                std::string currentCondition = "";
                    if(first) {
                        currentCondition = "WHERE ";
                        first = false;
                    } else {
                        currentCondition = " AND ";
                    }
                    currentCondition += ("\"" + fkeys[tableName][dep]);
                    currentCondition += "\" IN (";

                if(values.size() > 0) {
                    currentCondition += valuesFromVector(values);
                } else currentCondition += "NULL";
                currentCondition += ")";
                whereCondition += currentCondition;
            }
            return whereCondition;
        };

        int64_t totalRows = 0;
        auto runTable = [&](const std::string& tableName) {
            std::string query = R"(
                SELECT
                    *
                FROM 
            )" + tableName
            + R"(
            )" +
            whereCondition(tableName);
            std::cout << query << "\n";
            std::string copyQuery = "COPY(" + query + ") TO '/tmp/" + tableName + ".csv' WITH DELIMITER ',' CSV";
            std::cout << "copy Query: " << copyQuery << '\n';
            bool ran = false;
            bool first = true;
            std::vector<std::string> colNames;
            /*
            conn.execute([&](auto&& r)
            {   
                using dmitigr::pgfe::to;
                std::vector<std::string> values;
                 std::string insertQuery;
                if(!ran) {
                    ran = true;
                    for(size_t i = 0; i < r.field_count(); i++) {
                        std::string value = to<std::string>(r[i]);
                        std::string fieldName(r.field_name(i));
                        if(pgDataTypeNeedsEnclosedQuotes(tableCols[tableName][fieldName].dataType) && value != "") {
                            std::string enclose = "'";
                            if(tableCols[tableName][fieldName].dataType == PGDataType::CHARACTERVARYING) {
                            bool hasSingleQuote = false;
                                for(auto& chr : value) {
                                    if(chr == '\'') {
                                        hasSingleQuote = true;
                                        break;
                                    }
                                }
                                if(hasSingleQuote) enclose =" $$ ";
                            }
                            value = enclose + value + enclose;
                        } else if(tableCols[tableName][fieldName].dataType == PGDataType::BOOLEAN) {
                            value = value == "t" ? "TRUE" : "FALSE";
                        }
                        values.push_back(value != "" ? value : "NULL");
                        if(first) {
                            colNames.push_back(fieldName);
                        }
                    }
                    //std::cout << '\n';
                    insertQuery = "INSERT INTO " + tableName + " ( " + valuesFromVector(colNames) + " ) " + "VALUES ( " + valuesFromVector(values) + " ) ON CONFLICT DO NOTHING";
                    outfile << valuesFromVector(values) << '\n';
                    std::cout << insertQuery << '\n';
                    //local.execute([&](auto&& r) {
                    //}, insertQuery);
                    first = false;
                }
                totalRows++;
                for(auto& col : tableFkeyNeeds[tableName]) {
                    auto colVal = to<std::string>(r[col]);
                    tableColValues[tableName][col].push_back(colVal);
                }
            },
            copyQuery);
            */
        };

        std::cout << "<-------------------------------------------->\nORDER:\n";
        for(auto& l : L) {
            //runTable(l);
            std::cout << l << '\n';
        }

        std::cout << "<--------------->\nDirect Descendants:\n";
        for(auto& entry : directDescendants) {
            if(entry.second) {
                std::cout << entry.first << '\n';
            }
        }

        std::cout << "<--------------->\nNOT Direct Descendants:\n";
        for(auto& table : others) {
            std::cout << table << '\n';
        }
        
        std::cout << "<--------------------------------------------------\nDATA SEARCH:\n";
        std::vector<std::string> descendantSet = {};
        for(auto& l : L) {
            if(directDescendants.count(l) > 0 && directDescendants[l]) {
                std::cout << l << '\n';
                descendantSet.push_back(l);
            }
        }

        for(auto& l : othersL) {
            std::cout << l << '\n';
        }

        assert(descendantSet.size() > 0 && descendantSet[0] == std::string(argv[1]));
        
        auto dataSearchDescendantWhere = [&](const std::string& tableName){
            std::string where = "WHERE 1 = 1";
            bool flag = false;
            for(auto& dependencyTable : deps[tableName]) {
                if(directDescendants.count(dependencyTable) == 0 || !directDescendants[dependencyTable]) continue;
                std::string foreignKey = tableDependencyFKeys[tableName][dependencyTable];
                std::vector<std::string> values = tableColValues[dependencyTable][fkeyCols[dependencyTable][fkeys[tableName][dependencyTable]]];
                if(values.size()) {
                    flag = true;
                    where += (" AND " + foreignKey + " IN " + "(" + valuesFromVector(values) + ")");
                }
            }
            if(!flag) where += " AND 1 = 2";
            return where;
        };

        auto dataSearchTable = [&](const std::string& tableName){
            
            std::string query = "SELECT * FROM " + tableName + " " + dataSearchDescendantWhere(tableName);
            return query;
        };  
        fs::path dataDirectory = "data";
        fs::create_directory(dataDirectory);
        fs::current_path(dataDirectory);

        for(auto& descendantTableName : descendantSet) {
            fs::path tableDir = descendantTableName;
            fs::create_directory(tableDir);
        }

        for(auto& descendantTableName : descendantSet) {
            fs::path tableDir = descendantTableName;
            fs::current_path(tableDir);
            fs::path dataSearchPath = "data_search";
            fs::create_directory(dataSearchPath);
            fs::create_directory(fs::path("raw"));
            fs::current_path(dataSearchPath);
            std::string query = dataSearchTable(descendantTableName);
            std::cout << "-------------------------\n" <<  query << '\n';
            std::ofstream fout(descendantTableName + ".csv");
            bool firstRow = true;
            conn.execute([&](auto&& r)
            {
                using dmitigr::pgfe::to;
                bool firstCol = true;
                if(firstRow) {
                    bool innerFirstCol = true;
                    for(auto& col : r) {
                        bool needThisCol = false;
                        for(auto& fKey : tableFkeyNeeds[descendantTableName]) {
                             if(fKey == col.first) {
                                 needThisCol = true;
                                 break;
                             }
                        }
                        if(!needThisCol) continue;
                        if(!innerFirstCol) {
                            fout << ',';
                        } else {
                            innerFirstCol = false;
                        }
                        fout << col.first;
                    }
                    firstRow = false;
                    fout << std::endl;
                }
                for(auto& col : r) {
                    fs::current_path(fs::current_path().parent_path());
                    fs::current_path(fs::path("raw"));
                    bool needThisCol = false;
                    for(auto& fKey : tableFkeyNeeds[descendantTableName]) {
                         if(fKey == col.first) {
                             needThisCol = true;
                             break;
                         }
                    }
                    if(!needThisCol) continue;
                    if(!firstCol){
                        fout << ',';
                    } else {

                        firstCol = false;
                    }
                    fout << to<std::string>(r[col.first]);
                }
                fout << std::endl;
            },
            (query));
            fs::current_path(fs::current_path().parent_path().parent_path());
        }

        std::chrono::time_point afterTime = std::chrono::steady_clock::now();
        std::chrono::duration<float> elapsedTime = afterTime - beforeTime;
        std::cout << "Program ran in: " << elapsedTime << '\n';
        std::cout << "Total Number of Rows: " << totalRows << '\n';
        std::string foo = "foo";
        auto stringMaxSize = foo.max_size();
        std::cout << "Max string size: " << stringMaxSize << '\n';
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
