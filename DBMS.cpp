//
// Created by JakoError on 2022/11/10.
//
#include "DBMS.hpp"

#include <boost/exception/diagnostic_information.hpp>

#include <iostream>

#include "DBMSExceptions.hpp"

using namespace boost::filesystem;

namespace cppDBMS {


    const string DBMS::FAIL_MSG = "failed: ";
    const string DBMS::SUCC_MSG = "succeed: ";

    string DBMS::process_sql(const string &cmd) {
        try {
            if (regex_match(cmd, SQLParser::re_create_db)) {
                return process_create_db(cmd);
            } else if (regex_match(cmd, SQLParser::re_drop_db)) {
                return process_drop_db(cmd);
            } else if (regex_match(cmd, SQLParser::re_use_db)) {
                return process_use_db(cmd);
            } else if (regex_match(cmd, SQLParser::re_create_tb)) {
                return process_create_tb(cmd);
            } else if (regex_match(cmd, SQLParser::re_drop_tb)) {
                return process_drop_tb(cmd);
            } else if (regex_match(cmd, SQLParser::re_select)) {
                return process_select(cmd);
            } else if (regex_match(cmd, SQLParser::re_delete)) {
                return process_delete(cmd);
            } else if (regex_match(cmd, SQLParser::re_insert)) {
                return process_insert(cmd);
            } else {
                return "Syntax Wrong!";
            }
        } catch (std::exception const &x) {
            std::cerr << boost::diagnostic_information(x) << std::endl;
            std::flush(std::cerr);
            return "DBMS process Error:" + boost::diagnostic_information(x);
        }
    }

    void DBMS::load_data() {
        this->databases.clear();
        this->name_to_database.clear();
        for (const auto &entry: directory_iterator(get_data_path())) {
            if (is_directory(entry))
                this->databases.emplace_back(entry.path(), entry.path().filename().string());
        }
        for (auto &db: databases) {
            name_to_database[db.db_name] = &db;
        }
    }

    bool DBMS::is_db_exists(const string &db_name) {
        return name_to_database.count(db_name) != 0;
    }

    void DBMS::create() {
    }

    void DBMS::drop() {
    }

    void DBMS::save_data() {
    }

    void DBMS::release_data() {
    }

    vector<DataBase> &DBMS::getDatabases() {
        load_data();
        return databases;
    }

    DataBase *DBMS::getDatabase(const string &db_name) {
        if (is_db_exists(db_name))
            return name_to_database[db_name];
        return nullptr;
    }

    string DBMS::create_database(const string &db_name) {
        try {
            if (is_db_exists(db_name))
                BOOST_THROW_EXCEPTION(SqlException("Database " + db_name + " already exists!"));
            DataBase(get_data_path() / db_name, db_name).create();
        } catch (cppDBMSException &e) {
            std::cout << boost::diagnostic_information(e) << std::endl;
            return FAIL_MSG + e.getName() + ": " + e.what();
        }
        return SUCC_MSG + "Database " + db_name + " created.";
    }

    string DBMS::drop_database(const string &db_name) {
        try {
            DataBase *db = getDatabase(db_name);
            if (db == nullptr)
                BOOST_THROW_EXCEPTION(SqlException("Database " + db_name + " not exists!"));
            db->drop();
            //reload data to check existence
            if (is_db_exists(db_name))
                BOOST_THROW_EXCEPTION(SystemException("Database " + db_name + " drop failed!"));
        } catch (cppDBMSException &e) {
            std::cout << boost::diagnostic_information(e) << std::endl;
            return FAIL_MSG + e.getName() + ": " + e.what();
        }
        return SUCC_MSG + "Database " + db_name + " dropped.";
    }

    string DBMS::use_database(const string &db_name) {
        try {
            if (current_database != nullptr)

                current_database = getDatabase(db_name);
            if (current_database == nullptr)
                BOOST_THROW_EXCEPTION(SqlException("Database " + db_name + " not exists!"));
        } catch (cppDBMSException &e) {
            std::cout << boost::diagnostic_information(e) << std::endl;
            return FAIL_MSG + e.getName() + ": " + e.what();
        }
        return SUCC_MSG + "using Database " + db_name;
    }

    string DBMS::create_table(const string &tb_name,
                              const vector<string> &column_names, const vector<type_num_type> &column_types,
                              int primary_index) {
        try {
            if (current_database == nullptr)
                BOOST_THROW_EXCEPTION(SqlException("use Database first!"));
            current_database->create_table(tb_name, column_names, column_types, primary_index);
        } catch (cppDBMSException &e) {
            std::cout << boost::diagnostic_information(e) << std::endl;
            return FAIL_MSG + e.getName() + ": " + e.what();
        }
    }

    string DBMS::drop_table(const string &tb_name) {
        try {
            if (current_database == nullptr)
                BOOST_THROW_EXCEPTION(SqlException("use Database first!"));
            if (current_database->is_tb_exists(tb_name))
                BOOST_THROW_EXCEPTION(SqlException(
                                              "Table " + tb_name + " not exists in Database " +
                                              current_database->db_name + "!"));
            current_database->getTable(tb_name)->drop();
        } catch (cppDBMSException &e) {
            std::cout << boost::diagnostic_information(e) << std::endl;
            return FAIL_MSG + e.getName() + ": " + e.what();
        }
        return SUCC_MSG + "Database " + tb_name + " dropped.";
    }

    string DBMS::select_value(const string &tb_name, const vector<string> &columns) {
        try {
            if (current_database == nullptr)
                BOOST_THROW_EXCEPTION(SqlException("use Database first!"));
            if (current_database->is_tb_exists(tb_name))
                BOOST_THROW_EXCEPTION(SqlException("Table " + tb_name + " not exists in Database " +
                                                   current_database->db_name + "!"));

            auto tb = current_database->getTable(tb_name);
            tb->load_data();
            return tb->data_tostring(Table::ALL_LINE, columns);
        } catch (cppDBMSException &e) {
            std::cout << boost::diagnostic_information(e) << std::endl;
            return FAIL_MSG + e.getName() + ": " + e.what();
        }
    }

    template<typename value_type>
    string DBMS::select_value(const string &tb_name, const vector<string> &columns, const string &cond_col_name,
                              boost::function<bool(value_type)> cond) {
        try {
            if (current_database == nullptr)
                BOOST_THROW_EXCEPTION(SqlException("use Database first!"));
            if (current_database->is_tb_exists(tb_name))
                BOOST_THROW_EXCEPTION(SqlException("Table " + tb_name + " not exists in Database " +
                                                   current_database->db_name + "!"));

            auto tb = current_database->getTable(tb_name);
            tb->load_data();
            return tb->data_tostring(Table::ALL_LINE, columns);
        } catch (cppDBMSException &e) {
            std::cout << boost::diagnostic_information(e) << std::endl;
            return FAIL_MSG + e.getName() + ": " + e.what();
        }
    }
}