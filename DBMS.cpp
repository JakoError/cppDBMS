//
// Created by JakoError on 2022/11/10.
//
#include "DBMS.hpp"

#include <boost/exception/diagnostic_information.hpp>
#include <boost/lexical_cast.hpp>

#include <iostream>

#include "DBMSExceptions.hpp"

using namespace boost::filesystem;

namespace cppDBMS {


    const string DBMS::FAIL_MSG = "sql failed: ";
    const string DBMS::SUCC_MSG = "sql succeed: ";

    string DBMS::process_sql(string cmd) {
        try {
            boost::trim(cmd);
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
        } catch (cppDBMSException &e) {
            std::cout << boost::diagnostic_information(e) << std::endl;
            return FAIL_MSG + e.getName() + ": " + e.what();
        } catch (std::exception const &x) {
            std::cerr << boost::diagnostic_information(x) << std::endl;
            std::flush(std::cerr);
            return "DBMS process Error:" + boost::diagnostic_information(x);
        }
    }

    void DBMS::load_data() {
        for (const auto &entry: directory_iterator(get_data_path())) {
            auto db_name = entry.path().filename().string();
            if (is_directory(entry) && name_to_database.count(db_name) == 0) {
                name_to_database[db_name] = &(this->databases.emplace_back(entry.path(), db_name));
            }
        }
        name_to_database.clear();
        for (DataBase &db: databases) {
            name_to_database[db.db_name] = &db;
        }
    }

    bool DBMS::is_db_exists(const string &db_name) {
        load_data();
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
            load_data();
            if (is_db_exists(db_name))
                BOOST_THROW_EXCEPTION(SqlException("Database " + db_name + " already exists!"));
            add_database_record({get_data_path() / db_name, db_name}).create();
        } catch (cppDBMSException &e) {
            std::cout << boost::diagnostic_information(e) << std::endl;
            return FAIL_MSG + e.getName() + ": " + e.what();
        }
        return SUCC_MSG + "Database " + db_name + " created.";
    }

    string DBMS::drop_database(const string &db_name) {
        try {
            load_data();
            DataBase *db = getDatabase(db_name);
            if (current_database == db)
                current_database = nullptr;
            if (db == nullptr)
                BOOST_THROW_EXCEPTION(SqlException("Database " + db_name + " not exists!"));
            //drop file
            db->drop();
            //drop record
            drop_database_record(db_name);
        } catch (cppDBMSException &e) {
            std::cout << boost::diagnostic_information(e) << std::endl;
            return FAIL_MSG + e.getName() + ": " + e.what();
        }
        return SUCC_MSG + "Database " + db_name + " dropped.";
    }

    string DBMS::use_database(const string &db_name) {
        try {
            load_data();
            if (current_database != nullptr) {
                //todo 多用户优化release
                current_database->release_data();
            }
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
            //proxy to database
            current_database->create_table(tb_name, column_names, column_types, primary_index);
        } catch (cppDBMSException &e) {
            std::cout << boost::diagnostic_information(e) << std::endl;
            return FAIL_MSG + e.getName() + ": " + e.what();
        }
        return SUCC_MSG + "create Table " + tb_name + " succeed.";
    }

    string DBMS::drop_table(const string &tb_name) {
        try {
            if (current_database == nullptr)
                BOOST_THROW_EXCEPTION(SqlException("use Database first!"));
            //proxy to database
            current_database->drop_table(tb_name);
        } catch (cppDBMSException &e) {
            std::cout << boost::diagnostic_information(e) << std::endl;
            return FAIL_MSG + e.getName() + ": " + e.what();
        }
        return SUCC_MSG + "Database " + tb_name + " dropped.";
    }

    string DBMS::select_values(const string &tb_name, const vector<string> &columns) {
        try {
            if (current_database == nullptr)
                BOOST_THROW_EXCEPTION(SqlException("use Database first!"));
            if (!current_database->is_tb_exists(tb_name))
                BOOST_THROW_EXCEPTION(SqlException("Table " + tb_name + " not exists in Database " +
                                                   current_database->db_name + "!"));
            //direct call
            auto tb = current_database->getTable(tb_name);
            tb->load_data();
            return tb->data_tostring(Table::ALL_LINE, columns);
        } catch (cppDBMSException &e) {
            std::cout << boost::diagnostic_information(e) << std::endl;
            return FAIL_MSG + e.getName() + ": " + e.what();
        }
    }

    template<class T>
    string DBMS::select_value(const string &tb_name, const vector<string> &columns, const string &cond_col_name,
                              const boost::function<bool(const char *)> &cond, const string &cond_op, T value) {
        try {
            if (current_database == nullptr)
                BOOST_THROW_EXCEPTION(SqlException("use Database first!"));
            if (!current_database->is_tb_exists(tb_name))
                BOOST_THROW_EXCEPTION(SqlException("Table " + tb_name + " not exists in Database " +
                                                   current_database->db_name + "!"));
            //direct call
            auto tb = current_database->getTable(tb_name);
            tb->load_data();
            if (cond_op == "=" && tb->get_column_idx(cond_col_name) == tb->primary)
                return tb->data_tostring({tb->index[hash(value)]}, columns);
            else
                return tb->data_tostring(tb->cond_on_data(cond_col_name, cond), columns);
        } catch (cppDBMSException &e) {
            std::cout << boost::diagnostic_information(e) << std::endl;
            return FAIL_MSG + e.getName() + ": " + e.what();
        }
    }

    string DBMS::delete_value(const string &tb_name) {
        try {
            if (current_database == nullptr)
                BOOST_THROW_EXCEPTION(SqlException("use Database first!"));
            if (!current_database->is_tb_exists(tb_name))
                BOOST_THROW_EXCEPTION(SqlException("Table " + tb_name + " not exists in Database " +
                                                   current_database->db_name + "!"));
            //direct call
            auto tb = current_database->getTable(tb_name);
            tb->load_data();
            auto count = tb->delete_data();
            return SUCC_MSG + "delete " + std::to_string(count) + " line(s) of data in Table " + tb_name +
                   " -> remain " + std::to_string(tb->line_length) + " line(s) of data!";
        } catch (cppDBMSException &e) {
            std::cout << boost::diagnostic_information(e) << std::endl;
            return FAIL_MSG + e.getName() + ": " + e.what();
        }
    }

    string DBMS::delete_value(const string &tb_name, const string &cond_col_name,
                              const boost::function<bool(const char *)> &cond) {
        try {
            if (current_database == nullptr)
                BOOST_THROW_EXCEPTION(SqlException("use Database first!"));
            if (!current_database->is_tb_exists(tb_name))
                BOOST_THROW_EXCEPTION(SqlException("Table " + tb_name + " not exists in Database " +
                                                   current_database->db_name + "!"));
            //direct call
            auto tb = current_database->getTable(tb_name);
            tb->load_data();
            auto count = tb->delete_data(tb->cond_on_data(cond_col_name, cond));
            return SUCC_MSG + "delete " + std::to_string(count) + " line(s) of data in Table " + tb_name +
                   " -> remain " + std::to_string(tb->line_length) + " line(s) of data!";
        } catch (cppDBMSException &e) {
            std::cout << boost::diagnostic_information(e) << std::endl;
            return FAIL_MSG + e.getName() + ": " + e.what();
        }
    }

    string DBMS::insert_value(const string &tb_name, const vector<string> &value_strs,
                              const vector<type_num_type> &value_types) {
        size_type count;
        try {
            if (current_database == nullptr)
                BOOST_THROW_EXCEPTION(SqlException("use Database first!"));
            if (!current_database->is_tb_exists(tb_name))
                BOOST_THROW_EXCEPTION(SqlException("Table " + tb_name + " not exists in Database " +
                                                   current_database->db_name + "!"));
            //direct call
            auto tb = current_database->getTable(tb_name);
            tb->load_data();

            count = tb->insert_data(value_strs, value_types);
        } catch (cppDBMSException &e) {
            std::cout << boost::diagnostic_information(e) << std::endl;
            return FAIL_MSG + e.getName() + ": " + e.what();
        }
        return SUCC_MSG + " inserted " + std::to_string(count) + " line(s) of data to Table " + tb_name;
    }
}

namespace cppDBMS {
    string DBMS::process_create_db(const string &cmd) {
        //create database <database name>
        auto strs = SQLParser::split_by_space(cmd);
        if (strs.size() != 3)
            BOOST_THROW_EXCEPTION(ParserException("syntax parser match create database error"));
        return create_database(*strs.rbegin());
    }

    string DBMS::process_drop_db(const string &cmd) {
        //drop database <database name>
        auto strs = SQLParser::split_by_space(cmd);
        if (strs.size() != 3)
            BOOST_THROW_EXCEPTION(ParserException("syntax match drop database error"));
        return drop_database(*strs.rbegin());
    }

    string DBMS::process_use_db(const string &cmd) {
        //use <database name>
        auto strs = SQLParser::split_by_space(cmd);
        if (strs.size() != 2)
            BOOST_THROW_EXCEPTION(ParserException("syntax match use database error"));
        return use_database(*strs.rbegin());
    }

    string DBMS::process_create_tb(const string &cmd) {
        //create table <table-name> ( <column> <type> [primary],... )
        std::vector<string> strs;
        boost::split(strs, cmd, boost::is_any_of("()"));
        if (strs.size() != 3)
            BOOST_THROW_EXCEPTION(ParserException("syntax match create table error"));
        auto head_strs = SQLParser::split_by_space(*strs.begin());
        std::vector<string> param_strs;
        boost::split(param_strs, strs[1], boost::is_any_of(","));

        std::vector<string> column_names;
        std::vector<type_num_type> column_types;
        int primary_index = -1;
        for (int i = 0; i < param_strs.size(); ++i) {
            boost::trim(param_strs[i]);
            auto param = SQLParser::split_by_space(param_strs[i]);
            if (param.size() != 2 && param.size() != 3)
                BOOST_THROW_EXCEPTION(ParserException("syntax match create table column param error"));
            if (param.size() == 3) {
                if (primary_index == -1)
                    primary_index = i;
                else
                    BOOST_THROW_EXCEPTION(SqlException("cannot declare two or more primary key(s)"));
            }
            column_names.push_back(param[0]);
            column_types.push_back(param[1] == "int" ? int_type_num : str_type_num);
        }
        return create_table(*head_strs.rbegin(), column_names, column_types, primary_index);
    }

    string DBMS::process_drop_tb(const string &cmd) {
        //drop table <table-name>
        auto strs = SQLParser::split_by_space(cmd);
        if (strs.size() != 3)
            BOOST_THROW_EXCEPTION(ParserException("syntax match drop table error"));
        return drop_table(*strs.rbegin());
    }

    string DBMS::process_select(const string &cmd) {
        //select <column>|*|<columns> from <table> [ where <cond> ]
        //<cond> = <column> <op> <const-value>
        //<columns> = column1 , ...

        //pick string with "" first in case of wrong regex read
        auto str_value_l_idx = cmd.find_first_of('\"');
        auto str_value_r_idx = cmd.find_last_of('\"');
        string head_str = cmd.substr(0, str_value_l_idx);
        string value_str;
        if (str_value_l_idx != string::npos && str_value_r_idx != string::npos)
            value_str = cmd.substr(str_value_l_idx + 1, str_value_r_idx - str_value_l_idx - 1);

        boost::trim(head_str);
        boost::trim(value_str);
        //pick columns
        auto columns_strs = SQLParser::extract_columns(head_str);
        //-remain 0select 1from 2<table> [ 3where 4<column> 5<op> 6![<const-value>]]
        auto head_strs = SQLParser::split_by_space(head_str);

        if (columns_strs.empty()) {
            columns_strs = {head_strs[1]};
            head_strs.erase(head_strs.begin() + 1);
        }

        if (head_strs.size() == 3) {
            //0select 1from 2<table>
            return select_values(head_strs[2], columns_strs);
        } else if (head_strs.size() == 6) {
            boost::function<bool(const char *)> cond;
            if (head_strs[5] == "<")
                cond = [value_str](const char *val) { return string(val) < value_str; };
            else if (head_strs[5] == ">")
                cond = [value_str](const char *val) { return string(val) > value_str; };
            else {
                cond = [value_str](const char *val) { return string(val) == value_str; };
            }
            return select_value(head_strs[2], columns_strs, head_strs[4], cond, head_strs[5], value_str);
        } else if (head_strs.size() == 7) {
            int value_int = boost::lexical_cast<int>(head_strs[6]);
            boost::function<bool(const char *)> cond;
            if (head_strs[5] == "<")
                cond = [value_int](const char *val) { return *reinterpret_cast<const int *>(val) < value_int; };
            else if (head_strs[5] == ">")
                cond = [value_int](const char *val) { return *reinterpret_cast<const int *>(val) > value_int; };
            else
                cond = [value_int](const char *val) { return *reinterpret_cast<const int *>(val) == value_int; };
            return select_value(head_strs[2], columns_strs, head_strs[4], cond, head_strs[5], value_int);
        } else {
            BOOST_THROW_EXCEPTION(ParserException("syntax match select value error"));
        }
    }

    string DBMS::process_delete(const string &cmd) {
        //delete <table> [ where <column> <op> <const-value>]
        auto str_value_l_idx = cmd.find_first_of('\"');
        auto str_value_r_idx = cmd.find_last_of('\"');
        string head_str = cmd.substr(0, str_value_l_idx);
        string value_str;
        if (str_value_l_idx != string::npos && str_value_r_idx != string::npos && str_value_r_idx - str_value_l_idx > 2)
            value_str = cmd.substr(str_value_l_idx + 1, str_value_r_idx - str_value_l_idx - 1);

        boost::trim(head_str);
        boost::trim(value_str);

        auto head_strs = SQLParser::split_by_space(head_str);

        //-remain 0delete 1<table> [ 2where 3<column> 4<op> 5![<const-value>]]
        if (head_strs.size() == 2)
            return delete_value(head_strs[1]);
        else if (head_strs.size() == 5) {
            boost::function<bool(const char *)> cond;
            if (head_strs[4] == "<")
                cond = [value_str](const char *val) { return string(val) < value_str; };
            else if (head_strs[4] == ">")
                cond = [value_str](const char *val) { return string(val) > value_str; };
            else
                cond = [value_str](const char *val) { return string(val) == value_str; };
            return delete_value(head_strs[1], head_strs[3], cond);
        } else if (head_strs.size() == 6) {
            int value_int = boost::lexical_cast<int>(head_strs[5]);
            boost::function<bool(const char *)> cond;
            if (head_strs[4] == "<")
                cond = [value_int](const char *val) { return *reinterpret_cast<const int *>(val) < value_int; };
            else if (head_strs[4] == ">")
                cond = [value_int](const char *val) { return *reinterpret_cast<const int *>(val) > value_int; };
            else
                cond = [value_int](const char *val) { return *reinterpret_cast<const int *>(val) == value_int; };
            return delete_value(head_strs[1], head_strs[3], cond);
        } else {
            BOOST_THROW_EXCEPTION(ParserException("syntax match delete table error"));
        }
    }

    string DBMS::process_insert(const string &cmd) {
        //insert person values (1234, "123aaa", "aaa,bbb", " aaabbb \\" )
        std::vector<string> strs;
        auto brac_split_idx = cmd.find_first_of('(');
        if (brac_split_idx == string::npos)
            BOOST_THROW_EXCEPTION(ParserException("insert value parser wrong"));
        string head_str = cmd.substr(0, brac_split_idx);
        string trail_str = cmd.substr(brac_split_idx + 1);

        auto head_strs = SQLParser::split_by_space(head_str);
        auto tb_name = head_strs[1];

        vector<type_num_type> value_types;
        vector<string> value_strs;
        bool in_str = false;
        for (int l = 0, r = 1; r < trail_str.size(); ++r) {
            if (in_str) {
                if (trail_str[r] == '\"') {
                    in_str = false;
                    value_types.push_back(str_type_num);
                }
                continue;
            }
            if (trail_str[r] == '\"') {
                in_str = true;
                continue;
            }
            if (trail_str[r] == ',' || trail_str[r] == ')') {
                //segmentation
                value_strs.push_back(trail_str.substr(l, r - l));
                l = r + 1;
                if (value_strs.size() == value_types.size())
                    continue;
                else if (value_strs.size() == value_types.size() + 1)
                    value_types.push_back(int_type_num);
            }
        }
        //check two vector
        if (value_strs.size() != value_types.size())
            BOOST_THROW_EXCEPTION(ParserException("read values and it's types not match in insert sql: " + cmd));

//        for (auto &value_str: value_strs) {
//            boost::trim(value_str);
//        }
        //string remove ""
        for (size_t i = 0; i < value_strs.size(); ++i) {
            boost::trim(value_strs[i]);
            if (value_types[i] == str_type_num) {
                if (value_strs[i].length() < 2 || value_strs[i][0] != '\"' || value_strs[i].back() != '\"')
                    BOOST_THROW_EXCEPTION(ParserException("string value parse error"));
                value_strs[i] = value_strs[i].substr(1, value_strs[i].length() - 2);
            }
        }

        return insert_value(tb_name, value_strs, value_types);
    }
}