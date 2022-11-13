//
// Created by JakoError on 2022/11/10.
//
#ifndef DBMS_DATABASE_HPP
#define DBMS_DATABASE_HPP

#include <string>
#include <utility>

#include "Data.h"
#include "Table.hpp"

using std::string;

namespace cppDBMS {
    class DataBase : public Data {
    private:
        Table &add_table_record(Table &&tb) {
            Table &table = tables.emplace_back(tb);
            name_to_table[table.tb_name] = &table;
            return table;
        }

        void drop_table_record(const string &tb_name) {
            erase_if(tables, [tb_name](const Table &table) { return tb_name == table.tb_name; });
            name_to_table.erase(tb_name);
        }

    public:
        string db_name;
        vector<Table> tables;

        map<string, Table *> name_to_table;

        DataBase(const path &dataPath, string dbName) : Data(dataPath), db_name(std::move(dbName)) {}

        void load_data() override;

        void create() override;

        void drop() override;

        void save_data() override;

        void release_data() override;

        bool is_tb_exists(const string &tb_name);

        void create_table(const string &tb_name,
                          const vector<string> &column_names, const vector<type_num_type> &column_types,
                          size_type primary);

        void drop_table(const string &tb_name);

        Table *getTable(const string &tb_name);
    };
}


#endif //DBMS_DATABASE_HPP
