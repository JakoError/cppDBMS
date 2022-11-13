//
// Created by JakoError on 2022/11/10.
//

#ifndef DBMS_DBMS_HPP
#define DBMS_DBMS_HPP

#include "Data.h"
#include "DataBase.hpp"
#include "SQLParser.hpp"

#include <boost/regex.hpp>

#include <string>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

using boost::regex;

using std::string;

namespace cppDBMS {
    class DBMS : public Data {
    protected:
        static const string FAIL_MSG;
        static const string SUCC_MSG;

        vector<DataBase> databases;

        map<string, DataBase *> name_to_database;

        DataBase &add_database_record(DataBase &&db) {
            DataBase &database = databases.emplace_back(db);
            name_to_database[database.db_name] = &database;
            return database;
        }

        void drop_database_record(const string &db_name) {
            erase_if(databases, [db_name](const DataBase &database) { return db_name == database.db_name; });
            name_to_database.erase(db_name);
        }

    public:
        DataBase *current_database = nullptr;

        void create() override;

        void drop() override;

        void load_data() override;

        void save_data() override;

        void release_data() override;

        vector<DataBase> &getDatabases();

        DataBase *getDatabase(const string &db_name);

    private:
        bool is_db_exists(const string &db_name);

        string create_database(const string &db_name);

        string drop_database(const string &db_name);

        string use_database(const string &db_name);

        string create_table(const string &tb_name,
                            const vector<string> &column_names, const vector<type_num_type> &column_types,
                            int primary_index);

        string drop_table(const string &tb_name);

        string select_value(const string &tb_name, const vector<string> &columns);

        string select_value(const string &tb_name, const vector<string> &columns, const string &cond_col_name,
                            const boost::function<bool(const char *)> &cond);

        string delete_value(const string &tb_name);

        string delete_value(const string &tb_name, const string &cond_col_name,
                            const boost::function<bool(const char *)> &cond);

        string insert_value(const string &tb_name,
                            const vector<string> &value_strs, const vector<type_num_type> &value_types);

        string process_create_db(const string &cmd);

        string process_drop_db(const string &cmd);

        string process_use_db(const string &cmd);

        string process_create_tb(const string &cmd);

        string process_drop_tb(const string &cmd);

        string process_select(const string &cmd);

        string process_delete(const string &cmd);

        string process_insert(const string &cmd);

    public:
        explicit DBMS(const path &dataPath) : Data(dataPath) {};

        string process_sql(string cmd);
    };
}

#endif //DBMS_DBMS_HPP
