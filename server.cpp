//
// Created by JakoError on 2022/10/22.
//
#include <boost/regex.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/intrusive/bstree.hpp>
#include <boost/lexical_cast.hpp>

#include <string>
#include <utility>
#include <vector>
#include <iostream>
#include <utility>
#include <filesystem>
#include <fstream>

#define MAX_CMD_SIZE 1024

#define STR_LENGTH 256

using namespace boost::asio;
using boost::regex;
using boost::trim;

using namespace std::filesystem;
using std::cout;
using std::endl;
using std::string;
using std::vector;

class DBMS {
public:
    static const string FAIL_MSG;
    static const string SUCC_MSG;

    static string seg;
    static string seg_v;
    static string name_regex;
    static string column_regex;
    static string value_regex;
    static string cond_regex;
    static string where_regex;

    static regex re_create_db;
    static regex re_drop_db;
    static regex re_use_db;
    static regex re_create_tb;
    static regex re_drop_tb;
    static regex re_select;
    static regex re_delete;
    static regex re_insert;
    static regex re_exit;
private:
    enum cmd_num : int {
        create_db = 0, drop_db, use_db, create_tb, drop_tb, select_v, delete_v, insert_v, exit_c
    };

    enum type_num : int {
        int_type = 0, str_type = 1
    };

    std::filesystem::path db_path;

    string current_database;

    static vector<string> split_by_space(const string &str) {
        std::vector<string> strs;
        boost::split(strs, str, boost::is_any_of(" \t"), boost::token_compress_on);
        return strs;
    }

    bool is_db_exists(const string &db_name) {
        return std::ranges::any_of(directory_iterator(db_path),
                                   [&](const auto &entry) { return entry.path().filename() == db_name; });
//        for (const auto &entry: directory_iterator(db_path)) {
//            if (entry.path().filename() == db_name)
//                return true;
//        }
//        return false;
    }

    bool is_tb_exists(const string &tb_name) {
        if (current_database.empty())
            return false;
        return std::ranges::any_of(directory_iterator(db_path / current_database),
                                   [&](const auto &entry) { return entry.path().filename() == tb_name; });
//        for (const auto &entry: directory_iterator(db_path)) {
//            if (entry.path().filename() == db_name)
//                return true;
//        }
//        return false;
    }

    string create_database(const string &db_name) {
        std::fstream data;
        if (is_db_exists(db_name))
            return FAIL_MSG + "Database " + db_name + " already exists!";
        data.open(db_path / db_name);
        return SUCC_MSG + "Database " + db_name + " created.";
    }

    string drop_database(const string &db_name) {
        if (!is_db_exists(db_name))
            return FAIL_MSG + "Database " + db_name + " not exists!";
        remove_all(db_path / db_name);
        return SUCC_MSG + "Database " + db_name + " dropped.";
    }

    string use_database(const string &db_name) {
        if (!is_db_exists(db_name))
            return FAIL_MSG + "Database " + db_name + " not exists!";
        current_database = db_name;
        return SUCC_MSG + "using Database " + db_name;
    }

    string create_table(const string &tb_name, const vector<string> &column_names, const vector<type_num> &column_types,
                        int primary_index) {
        if (current_database.empty())
            return FAIL_MSG + " use Database first!";
        if (is_tb_exists(tb_name))
            return FAIL_MSG + "Table " + tb_name + " already exists in Database " + current_database + "!";
        if (column_names.size() != column_types.size())
            BOOST_THROW_EXCEPTION(std::runtime_error("column_names and column_types have different size!"));
        std::fstream data;
        //table info write
        data.open(db_path / current_database / tb_name / (tb_name + ".tb"));
//        data.write(reinterpret_cast<const char *>(&size), sizeof(size));
        data << column_types.size();
        for (auto &type: column_types) {
//            data.write(reinterpret_cast<const char *>(&type), sizeof(type));
            data << type;
        }
        //table column name write
        for (auto &name: column_names) {
            data << name.length() << name;
        }
        //table .dat create
        data.open(db_path / current_database / tb_name / (tb_name + ".dat"));
        //table .idx create
        if (primary_index != -1) {
            data.open(db_path / current_database / tb_name / (tb_name + ".idx"));
            data << primary_index;
        }
        return SUCC_MSG + "Table " + tb_name + " created!";
    }

    string drop_table(const string &tb_name) {
        if (current_database.empty())
            return FAIL_MSG + " use Database first!";
        if (!is_tb_exists(tb_name))
            return FAIL_MSG + "Table " + tb_name + " not exists in Database " + current_database + "!";
        remove_all(db_path / current_database / tb_name);
        //check tb directory delete
        if (is_tb_exists(tb_name))
            BOOST_THROW_EXCEPTION(std::runtime_error("server drop table io failed!"));
        return SUCC_MSG + "Database " + tb_name + " dropped.";
    }

    static void read_value(std::istream &is, int &value) {
        is >> value;
    }

    static void read_value(std::istream &is, string &value) {
        char buff[STR_LENGTH];
        is.read(buff, STR_LENGTH);
        string take(buff, STR_LENGTH);
        trim(take);
        value = take;
    }

    string select_value(const string &column, const string &tb_name) {
        if (current_database.empty())
            return FAIL_MSG + " use Database first!";
        if (!is_tb_exists(tb_name))
            return FAIL_MSG + "Table " + tb_name + " not exists in Database " + current_database + "!";
        std::fstream data;
        data.open(db_path / current_database / tb_name / (tb_name + ".tb"));
        size_t size = 0;
        if (data.eof())
            BOOST_THROW_EXCEPTION(std::runtime_error("broken table file!"));
        data >> size;
        if (size <= 0)
            BOOST_THROW_EXCEPTION(std::runtime_error("broken table file!"));
        vector<string> column_names;
        vector<int> column_types;
        for (size_t i = 0; i < size; ++i) {
            data >> column_types[i];
        }
        for (size_t i = 0; i < size; ++i) {
            typename string::size_type length;
            data >> length;
            boost::shared_ptr<char> buffer(new char[length]);
            data.read(buffer.get(), static_cast<std::streamsize>(length));
            column_names[i] = string(buffer.get(), length);
        }
        std::stringstream ss;
        ss << "Table:" << tb_name << endl;
        for (size_t i = 0; i < size; ++i) {
            ss << column_names[i] << "\t";
        }
        ss << "\n";
        data.open(db_path / current_database / tb_name / (tb_name + ".dat"));
        while (!data.eof()) {
            for (size_t i = 0; i < size; ++i) {
                if (column_types[i] == int_type) {
                    int value;
                    read_value(data, value);
                    ss << value << "\t";
                } else {
                    string value;
                    read_value(data, value);
                    ss << "\"" << value << "\"\t";
                }
            }
            ss << endl;
        }
        return ss.str();
    }

    template<typename value_type>
    string select_value(const string &col_name, const string &tb_name, const string &cond_col_name,
                        boost::function<bool(value_type)> cond) {
        if (current_database.empty())
            return FAIL_MSG + " use Database first!";
        if (!is_tb_exists(tb_name))
            return FAIL_MSG + "Table " + tb_name + " not exists in Database " + current_database + "!";
        std::fstream data;
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
            if (column_types[i] == int_type)
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
                if (!cond(value,)) continue;
            }
            if (select_col_idx != -1) {
                data.seekg(static_cast<std::istream::off_type>(column_offset[select_col_idx]),
                           static_cast<std::ios_base::seekdir>(line_idx * line_size));
                if (column_types[select_col_idx] == int_type) {
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
                    if (column_types[i] == int_type) {
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

    template<typename value_type>
    string delete_value(const string &tb_name) {
        if (current_database.empty())
            return FAIL_MSG + " use Database first!";
        if (!is_tb_exists(tb_name))
            return FAIL_MSG + "Table " + tb_name + " not exists in Database " + current_database + "!";
        std::fstream data;
        data.open(db_path / current_database / tb_name / (tb_name + ".dat"));
        return SUCC_MSG + "delete all the value in Table "+tb_name;
    }

    template<typename value_type>
    string delete_value(const string &tb_name, const string &cond_col_name, boost::function<bool(value_type)> cond) {
        if (current_database.empty())
            return FAIL_MSG + " use Database first!";
        if (!is_tb_exists(tb_name))
            return FAIL_MSG + "Table " + tb_name + " not exists in Database " + current_database + "!";
        std::fstream data;
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
            if (column_types[i] == int_type)
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
                if (!cond(value,)) continue;
            }
            if (select_col_idx != -1) {
                data.seekg(static_cast<std::istream::off_type>(column_offset[select_col_idx]),
                           static_cast<std::ios_base::seekdir>(line_idx * line_size));
                if (column_types[select_col_idx] == int_type) {
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
                    if (column_types[i] == int_type) {
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
            column_types.push_back(param[1] == "int" ? int_type : str_type);
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
        auto value_idx = cmd.find_first_of('\"');
        string head_str = cmd.substr(0, value_idx);
        string value_str;
        if (value_idx != string::npos)
            value_str = cmd.substr(value_idx);
        trim(head_str);
        trim(value_str);
        auto head_strs = split_by_space(head_str);

        if (head_strs.size() == 2)
            delete_value(head_strs[1]);
        else if (head_strs.size() == 5) {
            delete_value(head_strs[1], head_strs[3], cond_op_map[head_strs[4]], value_str);
        } else if (head_strs.size() == 6) {
            delete_value(head_strs[1], head_strs[3], cond_op_map[head_strs[4]], boost::lexical_cast<int>(head_strs[5]));
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

    string process_sql(const string &cmd) {
        try {
            if (regex_match(cmd, re_create_db)) {
                return process_create_db(cmd);
            } else if (regex_match(cmd, re_drop_db)) {
                return process_drop_db(cmd);
            } else if (regex_match(cmd, re_use_db)) {
                return process_use_db(cmd);
            } else if (regex_match(cmd, re_create_tb)) {
                return process_create_tb(cmd);
            } else if (regex_match(cmd, re_drop_tb)) {
                return process_drop_tb(cmd);
            } else if (regex_match(cmd, re_select)) {
                return process_select(cmd);
            } else if (regex_match(cmd, re_delete)) {
                return process_delete(cmd);
            } else if (regex_match(cmd, re_insert)) {
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
};

const string DBMS::FAIL_MSG = "operation fail: ";
const string DBMS::SUCC_MSG = "operation succeed: ";

string DBMS::seg = "[\\s\t]+";
string DBMS::seg_v = "[\\s\t]*";
string DBMS::name_regex = "([a-z]{1,63})";
string DBMS::column_regex = "(" + name_regex + seg + "((int)|(string))(" + seg + "primary)?)";
string DBMS::value_regex = "((0|[1-9][0-9]*)|(\".{0,256}\"))";
string DBMS::cond_regex = "(" + name_regex + seg_v + "[=<>]" + seg_v + value_regex + ")";
string DBMS::where_regex = "(" + seg + "where" + seg + cond_regex + ")";

regex DBMS::re_create_db = regex("create" + seg + "database" + seg + name_regex);
regex DBMS::re_drop_db = regex("drop" + seg + "database" + seg + name_regex);
regex DBMS::re_use_db = regex("use" + seg + name_regex);
regex DBMS::re_create_tb = regex("create" + seg + "table" + seg + name_regex + seg_v +
                                 "\\(" + seg_v + column_regex + "(" + seg_v + "," + seg_v + column_regex + ")+" +
                                 seg_v + "\\)");
regex DBMS::re_drop_tb = regex("drop" + seg + "table" + seg + name_regex);
regex DBMS::re_select = regex("select" + seg + "(" + name_regex + "|[*])" + seg + "from" + seg + name_regex
                              + where_regex + "?");
regex DBMS::re_delete = regex("delete" + seg + name_regex + where_regex + "?");
regex DBMS::re_insert = regex("insert" + seg + name_regex + seg + "values" + seg_v +
                              "\\(" + seg_v + "(" + value_regex + seg_v + "(," + seg_v + value_regex + seg_v + ")*)" +
                              "\\)");
regex DBMS::re_exit = regex("exit");


class DBMSServer {
private:
    typedef boost::shared_ptr<boost::asio::ip::tcp::socket> socket_ptr;

    int client_num = 0;

    path dbms_path;

    boost::mutex client_num_mutex;

    boost::mutex sql_mutex;

    //service instance
    io_service &ios;

    ip::tcp::endpoint ep;

    ip::tcp::acceptor acc;

    void client_exit(const socket_ptr &sock) {
        client_num_mutex.lock();
        client_num--;
        cout << "client leave--client_ip:" << sock->remote_endpoint().address()
             << ",client_port" << sock->remote_endpoint().port() << endl;
        client_num_mutex.unlock();
        sock->close();
    }

    void client_session(const socket_ptr &sock) {
        DBMS dbms(dbms_path);
        char data[MAX_CMD_SIZE];
        try {
            while (true) {
                memset(data, 0, MAX_CMD_SIZE);
                size_t len = sock->read_some(buffer(data));
                if (len > 0) {
                    write(*sock, buffer(dbms.process_sql(string(data))));
                }
            }
        } catch (boost::thread_interrupted &x) {
            client_exit(sock);
            return;
        } catch (boost::exception &x) {
            std::cerr << boost::diagnostic_information(x) << std::endl;
            client_exit(sock);
            return;
        }
    }

public:
    DBMSServer(path dbms_path, io_service &ios, ip::tcp protocal, ip::port_type port) :
            dbms_path(std::move(dbms_path)), ios(ios),
            ep(protocal, port),
            acc(ios, ep) {
    }

    [[noreturn]] void start() {
        while (true) {
            socket_ptr sock(new ip::tcp::socket(ios));
            acc.accept(*sock);
            cout << "accept new client:" << "clinet1" << " ip:" << sock->remote_endpoint().address() << endl;
            boost::thread([sock, this] { return client_session(sock); });
        }
    }
};

boost::asio::ip::port_type port = 8000;

string data_path = "./data/";

int main() {
    io_service ios;
    DBMSServer Server(path(data_path), ios, ip::tcp::v4(), port);
    Server.start();
}