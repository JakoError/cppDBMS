//
// Created by JakoError on 2022/10/22.
//
#include <boost/regex.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/intrusive/bstree.hpp>

#include <string>
#include <utility>
#include <vector>
#include <iostream>
#include <utility>
#include <filesystem>
#include <fstream>

#define MAX_CMD_SIZE 1024

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
    enum cmd_num {
        create_db = 0, drop_db, use_db, create_tb, drop_tb, select_v, delete_v, insert_v, exit_c
    };

    enum type_num {
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
            data.open(db_path / current_database / tb_name / (tb_name + ".data"));
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
        return SUCC_MSG + "Database " + tb_name + " dropped.";
    }

    string select_value(string &column, string &tb_name){

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
            select_value(head_strs[1], head_strs[3]);
        } else if (head_strs.size() == 7) {
            select_value(head_strs[1], head_strs[3], head_strs[5], cond_op_map[head_strs[6]], value_str);
        } else if (head_strs.size() == 8) {
            select_value(head_strs[1], head_strs[3], head_strs[5], cond_op_map[head_strs[6]],
                         boost::lexical_cast<int>(head_strs[7]));
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