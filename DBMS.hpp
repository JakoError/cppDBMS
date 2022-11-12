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

        string select_value(const string &tb_name,const vector<string> &columns);

        template<typename value_type>
        string select_value(const string &tb_name,const vector<string> &columns, const string &cond_col_name,
                            boost::function<bool(value_type)> cond);

        string delete_value(const string &tb_name) {
            if (current_database.empty())
                return FAIL_MSG + " use Database first!";
            if (!is_tb_exists(tb_name))
                return FAIL_MSG + "Table " + tb_name + " not exists in Database " + current_database + "!";
            std::fstream data;
            data.open(db_path / current_database / tb_name / (tb_name + ".dat"));
            return SUCC_MSG + "delete all the value in Table " + tb_name;
        }

        template<typename value_type>
        string
        delete_value(const string &tb_name, const string &cond_col_name, boost::function<bool(value_type)> cond) {
            if (current_database.empty())
                return FAIL_MSG + " use Database first!";
            if (!is_tb_exists(tb_name))
                return FAIL_MSG + "Table " + tb_name + " not exists in Database " + current_database + "!";
            auto tmp_dat_path = db_path / current_database / tb_name / (tb_name + ".tb" + ".tmp");
            auto dat_path = db_path / current_database / tb_name / (tb_name + ".tb");
            if (copy_file(dat_path, tmp_dat_path))
                BOOST_THROW_EXCEPTION(std::runtime_error("io operation failed"));

            std::fstream tmp;
            std::fstream data;
            tmp.open(tmp_dat_path, std::ios::in);
            data.open(db_path / current_database / tb_name / (tb_name + ".tb"));
            size_t size = 0;
            if (data.eof())
                BOOST_THROW_EXCEPTION(std::runtime_error("broken table file!"));
            data >> size;
            if (size <= 0)
                BOOST_THROW_EXCEPTION(std::runtime_error("broken table file!"));
            //define basic info about table
            vector<string> column_names(size);
            vector<int> column_types(size);
            size_t line_size = 0;
            vector<size_t> column_offset(size);

            //anchor of column
            int select_col_idx = -1;
            int cond_col_idx = -1;

            //read column type
            for (int i = 0; i < size; ++i) {
                data >> column_types[i];
                column_offset[i] = line_size;
                if (column_types[i] == int_type_num)
                    line_size += sizeof(int);
                else
                    line_size += sizeof(char) * STR_LENGTH;
            }
            //read column name & set anchor
            for (int i = 0; i < size; ++i) {
                typename string::size_type length;
                data >> length;
                boost::shared_ptr<char> buffer(new char[length]);
                data.read(buffer.get(), static_cast<std::streamsize>(length));
                column_names[i] = string(buffer.get(), length);
                if (cond_col_idx == -1 && cond_col_name == column_names[i])
                    cond_col_idx = i;
                if (select_col_idx == -1 && col_name == column_names[i])
                    select_col_idx = i;
            }
            if (col_name != "*" && select_col_idx == -1)
                return FAIL_MSG + "Column " + col_name + " not exists in Table " + tb_name;
            if (!cond_col_name.empty() && cond_col_idx == -1)
                return FAIL_MSG + "Column " + cond_col_name + " not exists in Table " + tb_name;

            //generate head output
            std::stringstream ss;
            ss << "Table:" << tb_name << endl;
            if (select_col_idx != -1)
                ss << column_names[select_col_idx];
            else
                for (int i = 0; i < size; ++i) {
                    ss << column_names[i] << "\t";
                }
            ss << endl;
            //generate data output
            data.open(db_path / current_database / tb_name / (tb_name + ".dat"));
            for (int line_idx = 0; !data.eof(); ++line_idx) {
                //cond
                if (cond_col_idx != -1) {
                    data.seekg(static_cast<std::istream::off_type>(column_offset[cond_col_idx]),
                               static_cast<std::ios_base::seekdir>(line_idx * line_size));
                    value_type value;
                    read_value(data, value);
                    if (!cond(value)) continue;
                }
                if (select_col_idx != -1) {
                    data.seekg(static_cast<std::istream::off_type>(column_offset[select_col_idx]),
                               static_cast<std::ios_base::seekdir>(line_idx * line_size));
                    if (column_types[select_col_idx] == int_type_num) {
                        int value;
                        data >> value;
                        ss << value;
                    } else {
                        char buff[STR_LENGTH];
                        data.read(buff, STR_LENGTH);
                        string value(buff, STR_LENGTH);
                        trim(value);
                        ss << value;
                    }
                } else {
                    for (size_t i = 0; i < size; ++i) {
                        if (column_types[i] == int_type_num) {
                            int value;
                            data >> value;
                            ss << value << "\t";
                        } else {
                            char buff[STR_LENGTH];
                            data.read(buff, STR_LENGTH);
                            string value(buff, STR_LENGTH);
                            trim(value);
                            ss << "\"" << value << "\"\t";
                        }
                    }
                }
                ss << endl;
            }
            return ss.str();
        }

        string process_create_db(const string &cmd) {
            auto strs = split_by_space(cmd);
            if (strs.size() != 3)
                BOOST_THROW_EXCEPTION(std::runtime_error("syntax match create database error"));
            return create_database(*strs.rbegin());
        }

        string process_drop_db(const string &cmd) {
            auto strs = split_by_space(cmd);
            if (strs.size() != 3)
                BOOST_THROW_EXCEPTION(std::runtime_error("syntax match drop database error"));
            drop_database(*strs.rbegin());
        }

        string process_use_db(const string &cmd) {
            auto strs = split_by_space(cmd);
            if (strs.size() != 2)
                BOOST_THROW_EXCEPTION(std::runtime_error("syntax match use database error"));
            use_database(*strs.rbegin());
        }

        string process_create_tb(const string &cmd) {
            std::vector<string> strs;
            boost::split(strs, cmd, boost::is_any_of("()"));
            if (strs.size() != 3)
                BOOST_THROW_EXCEPTION(std::runtime_error("syntax match create table error"));
            auto head_strs = split_by_space(*strs.begin());
            std::vector<string> param_strs;
            boost::split(param_strs, strs[1], boost::is_any_of(","));

            std::vector<string> column_names;
            std::vector<type_num> column_types;
            int primary_index = -1;
            for (int i = 0; i < param_strs.size(); ++i) {
                trim(param_strs[i]);
                auto param = split_by_space(param_strs[i]);
                if (param.size() != 2 && param.size() != 3)
                    BOOST_THROW_EXCEPTION(std::runtime_error("syntax match create table column param error"));
                if (param.size() == 3) {
                    if (primary_index == -1)
                        primary_index = i;
                    else
                        BOOST_THROW_EXCEPTION(std::runtime_error("不能存在两个及以上的primary key"));
                }
                column_names.push_back(param[0]);
                column_types.push_back(param[1] == "int" ? int_type_num : str_type_num);
            }
            create_table(*head_strs.rbegin(), column_names, column_types, primary_index);
        }

        string process_drop_tb(const string &cmd) {
            auto strs = split_by_space(cmd);
            if (strs.size() != 3)
                BOOST_THROW_EXCEPTION(std::runtime_error("syntax match drop table error"));
            drop_table(*strs.rbegin());
        }

        string process_select(const string &cmd) {
            auto value_idx = cmd.find_first_of('\"');
            string head_str = cmd.substr(0, value_idx);
            string value_str;
            if (value_idx != string::npos)
                value_str = cmd.substr(value_idx);
            trim(head_str);
            trim(value_str);
            auto head_strs = split_by_space(head_str);
            if (head_strs.size() == 4) {
                return select_value(head_strs[1], head_strs[3]);
            } else if (head_strs.size() == 7) {
                boost::function<bool(string)> cond;
                if (head_strs[6] == "<")
                    cond = [value_str](const string &val) { return val < value_str; };
                else if (head_strs[6] == ">")
                    cond = [value_str](const string &val) { return val > value_str; };
                else
                    cond = [value_str](const string &val) { return val == value_str; };
                return select_value<string>(head_strs[1], head_strs[3], head_strs[5], cond);
            } else if (head_strs.size() == 8) {
                int value_int = boost::lexical_cast<int>(head_strs[7]);
                boost::function<bool(int)> cond;
                if (head_strs[6] == "<")
                    cond = [value_int](int val) { return val < value_int; };
                else if (head_strs[6] == ">")
                    cond = [value_int](int val) { return val > value_int; };
                else
                    cond = [value_int](int val) { return val == value_int; };
                return select_value<int>(head_strs[1], head_strs[3], head_strs[5], cond);
            } else {
                BOOST_THROW_EXCEPTION(std::runtime_error("syntax match select value error"));
            }
        }

        string process_delete(const string &cmd) {
            auto value_idx = cmd.find_first_of('\"'); //delete <table> [ where "str" ]
            string head_str = cmd.substr(0, value_idx);//delete <table> [ where
            string value_str;
            if (value_idx != string::npos)
                value_str = cmd.substr(value_idx);//"str"
            trim(head_str);
            trim(value_str);
            auto head_strs = split_by_space(head_str);

            if (head_strs.size() == 2)
                delete_value(head_strs[1]);
            else if (head_strs.size() == 5) {
                boost::function<bool(string)> cond;
                if (head_strs[6] == "<")
                    cond = [value_str](const string &val) { return val < value_str; };
                else if (head_strs[6] == ">")
                    cond = [value_str](const string &val) { return val > value_str; };
                else
                    cond = [value_str](const string &val) { return val == value_str; };
                return delete_value<string>(head_strs[1], head_strs[3], cond);
            } else if (head_strs.size() == 6) {
                int value_int = boost::lexical_cast<int>(head_strs[5]);
                boost::function<bool(int)> cond;
                if (head_strs[6] == "<")
                    cond = [value_int](int val) { return val < value_int; };
                else if (head_strs[6] == ">")
                    cond = [value_int](int val) { return val > value_int; };
                else
                    cond = [value_int](int val) { return val == value_int; };
                return delete_value(head_strs[1], head_strs[3], cond);
            } else {
                BOOST_THROW_EXCEPTION(std::runtime_error("syntax match delete table error"));
            }
        }

        string process_insert(const string &cmd) {
            //insert person values (1234, "123aaa", "aaa,bbb", " aaabbb \\" )
            std::vector<string> strs;
            auto brac_split_idx = cmd.find_first_of('(');
            string head_str = cmd.substr(0, brac_split_idx);
            string trail_str = cmd.substr(brac_split_idx + 1);

            auto head_strs = split_by_space(head_str);
            auto &tb_name = head_strs[1];

            vector<string> value_strs;
            bool in_str = false;
            for (int l = 0, r = 1; r < trail_str.size(); ++r) {
                if (in_str) {
                    if (trail_str[r] == '\"')
                        in_str = false;
                    continue;
                }
                if (trail_str[r] == '\"') {
                    in_str = true;
                    continue;
                }
                if (trail_str[r] == ',' || trail_str[r] == ')') {
                    //分隔符
                    value_strs.push_back(trail_str.substr(l, r - l));
                    l = r + 1;
                }
            }
            for (auto &value_str: value_strs) {
                trim(value_str);
            }
            insert_value(tb_name, value_strs);
        }

    public:
        explicit DBMS(path dbPath) : db_path(std::move(dbPath)) {}

        string process_sql(const string &cmd);
    };
}

#endif //DBMS_DBMS_HPP
