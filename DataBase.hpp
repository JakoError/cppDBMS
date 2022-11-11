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
    public:
        string db_name;
        vector<Table> tables;

        DataBase(const path &dataPath, string dbName) : Data(dataPath), db_name(std::move(dbName)) {}

        void load_data() override;

        void create() override;

        void drop() override;

        bool is_tb_exists(const string &tb_name);

        void create_table(const string &tb_name,
                          const vector<string> &column_names, const vector<type_num> &column_types,
                          int primary_index);

        Table *getTable(const string &tb_name);
    };
}


#endif //DBMS_DATABASE_HPP
