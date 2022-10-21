#include <boost/regex.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/throw_exception.hpp>
#include <boost/exception/diagnostic_information.hpp>
#include <boost/lexical_cast.hpp>
#include <iostream>
#include <string>
#include <list>

using std::cout;
using std::cin;
using std::cerr;
using std::endl;
using std::string;
using std::getline;
using std::list;
using std::vector;

using boost::regex;
using boost::regex_match;
using boost::algorithm::trim;

typedef std::istream_iterator<int> in;

string cmd_line = "> ";

string seg = "[\\s\t]+";
string seg_v = "[\\s\t]*";
string name_regex = "([A-Za-z0-9]{1,63})";
string column_regex = "(" + name_regex + seg + "((int)|(string))(" + seg + "primary)?)";
string value_regex = "((0|[1-9][0-9]*)|(\".{0,256}\"))";
string cond_regex = "(" + name_regex + seg_v + "[=<>]" + seg_v + value_regex + ")";
string where_regex = "(" + seg + "where" + seg + cond_regex + ")";

enum type_num {
    int_type = 0, str_type = 1
};

enum cond_op_num {
    equal = 0, less, great
};

enum cmd_num {
    create_db = 0, drop_db, use_db, create_tb, drop_tb, select_v, delete_v, insert_v
};

std::map<string, cond_op_num> cond_op_map = {{"=", equal},
                                             {"<", less},
                                             {">", great}};

inline bool is_str_type(const string &str) {
    return *str.begin() == '\"' && *str.end() == '\"';
}

vector<string> split_by_space(const string &str);

void create_database(string &db_name);

void drop_database(string &db_name);

void use_database(string &db_name);

void create_table(string &tb_name, vector<string> &column_names, vector<type_num> &column_types, int primary_index);

void drop_table(string &tb_name);

void select_value(string &column, string &tb_name);

template<typename Type>
void select_value(string &column, string &tb_name, string &cond_column, cond_op_num cond_op, Type cond_value);

void delete_value(string &tb_name);

template<typename Type>
void delete_value(string &tb_name, string &cond_column, cond_op_num cond_op, Type cond_value);

void insert_value(string &tb_name, vector<string> &values);

void create_db_process(const string &cmd);

void drop_db_process(const string &cmd);

void use_db_process(const string &cmd);

void create_tb_process(const string &cmd);

void drop_tb_process(const string &cmd);

void select_process(const string &cmd);

void delete_process(const string &cmd);

void insert_process(string &cmd);

int main() {
    regex re_create_db("create" + seg + "database" + seg + name_regex);
    regex re_drop_db("drop" + seg + "database" + seg + name_regex);
    regex re_use_db("use" + seg + name_regex);
    regex re_create_tb(
            "create" + seg + "table" + seg + name_regex + seg_v + "\\(" + seg_v + column_regex + "(" + seg_v + "," +
            seg_v + column_regex + ")+" + seg_v + "\\)");
    regex re_drop_tb("drop" + seg + "table" + seg + "[A-Za-z0-9]{1,63}");
    regex re_select("select" + seg + "(" + name_regex + "|[*])" + seg + "from" + seg + name_regex + where_regex + "?");
    regex re_delete("delete" + seg + name_regex + where_regex + "?");
    regex re_insert("insert" + seg + name_regex + seg + "values" + seg_v + "\\(" + seg_v +
                    "(" + value_regex + seg_v + "(," + seg_v + value_regex + seg_v + ")*)"
                    + "\\)");
    regex re_exit("exit");
    while (true) {
        cout << cmd_line;
        string cmd;
        getline(cin, cmd);
        trim(cmd);
        try {
            if (regex_match(cmd, re_create_db)) {
                create_db_process(cmd);
            } else if (regex_match(cmd, re_drop_db)) {
                drop_db_process(cmd);
            } else if (regex_match(cmd, re_use_db)) {
                use_db_process(cmd);
            } else if (regex_match(cmd, re_create_tb)) {
                create_tb_process(cmd);
            } else if (regex_match(cmd, re_drop_tb)) {
                drop_tb_process(cmd);
            } else if (regex_match(cmd, re_select)) {
                select_process(cmd);
            } else if (regex_match(cmd, re_delete)) {
                delete_process(cmd);
            } else if (regex_match(cmd, re_insert)) {
                insert_process(cmd);
            } else if (regex_match(cmd, re_exit)) {
                exit(0);
            } else if (cmd.empty()) {
                continue;
            } else {
                std::flush(cout);
                cout << "Syntax Wrong!" << endl;
                std::flush(cout);
            }
        } catch (std::exception const &x) {
            std::cerr << boost::diagnostic_information(x) << std::endl;
            std::flush(std::cerr);
            system("pause");
        }
    }
}

void create_db_process(const string &cmd) {
    auto strs = split_by_space(cmd);
    if (strs.size() != 3)
        BOOST_THROW_EXCEPTION(std::runtime_error("syntax match create database error"));
    create_database(*strs.rbegin());
}

void drop_db_process(const string &cmd) {
    auto strs = split_by_space(cmd);
    if (strs.size() != 3)
        BOOST_THROW_EXCEPTION(std::runtime_error("syntax match drop database error"));
    drop_database(*strs.rbegin());
}

void use_db_process(const string &cmd) {
    auto strs = split_by_space(cmd);
    if (strs.size() != 2)
        BOOST_THROW_EXCEPTION(std::runtime_error("syntax match use database error"));
    use_database(*strs.rbegin());
}

void create_tb_process(const string &cmd) {
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

void drop_tb_process(const string &cmd) {
    auto strs = split_by_space(cmd);
    if (strs.size() != 3)
        BOOST_THROW_EXCEPTION(std::runtime_error("syntax match drop table error"));
    drop_table(*strs.rbegin());
}

void select_process(const string &cmd) {
    auto strs = split_by_space(cmd);
    if (strs.size() == 4) {
        select_value(strs[1], strs[3]);
    } else if (strs.size() == 8) {
        auto &value_str = strs[7];
        if (is_str_type(value_str)) {
            boost::algorithm::trim_if(value_str, boost::algorithm::is_any_of("\""));
            select_value(strs[1], strs[3], strs[5], cond_op_map[strs[6]], value_str);
        } else {
            select_value(strs[1], strs[3], strs[5], cond_op_map[strs[6]],
                         boost::lexical_cast<int>(value_str));
        }
    } else {
        BOOST_THROW_EXCEPTION(std::runtime_error("syntax match select value error"));
    }
}

void delete_process(const string &cmd) {
    auto strs = split_by_space(cmd);
    if (strs.size() == 2)
        delete_value(strs[1]);
    else if (strs.size() == 6) {
        auto &value_str = strs[5];
        if (is_str_type(value_str)) {
            boost::algorithm::trim_if(value_str, boost::algorithm::is_any_of("\""));
            delete_value(strs[1], strs[3], cond_op_map[strs[4]], value_str);
        } else {
            delete_value(strs[1], strs[3], cond_op_map[strs[4]], boost::lexical_cast<int>(value_str));
        }
    } else {
        BOOST_THROW_EXCEPTION(std::runtime_error("syntax match delete table error"));
    }
}

void insert_process(string &cmd) {
    std::vector<string> strs;
    auto brac_split_idx = cmd.find_first_of('(');
    string head_str = cmd.substr(0,brac_split_idx);
    string trail_str = cmd.substr(brac_split_idx+1);


    auto head_strs = split_by_space(head_str);
    auto &tb_name = head_strs[1];

    std::vector<string> value_strs;
    for (auto iter_r = trail_str.begin(),iter_l=iter_r; iter_r != trail_str.end()-1; ++iter_r) {

    }
    for (auto &value_str: value_strs) {
        trim(value_str);
    }
    insert_value(tb_name, value_strs);
}

vector<string> split_by_space(const string &str) {
    std::vector<string> strs;
    boost::split(strs, str, boost::is_any_of(" \t"), boost::token_compress_on);
    return strs;
}

void create_database(string &db_name) {
    cout<<"create db:"<<db_name<<endl;
}

void drop_database(string &db_name) {
    cout<<"drop db:"<<db_name<<endl;
}

void use_database(string &db_name) {
    cout<<"use db:"<<db_name<<endl;
}

void create_table(string &tb_name, vector<string> &column_names, vector<type_num> &column_types, int primary_index) {
    cout<<"create tb:"<<tb_name<<endl;
    for (int i = 0; i < column_names.size(); ++i) {
        cout<<i<<"\t:"<<column_names[i]<<" "<<column_types[i]<<endl;
    }
    cout<<"primary:"<<primary_index<<endl;
}

void drop_table(string &tb_name){
    cout<<"drop tb:"<<tb_name<<endl;
}

void select_value(string &column, string &tb_name){
    cout<<"select val:"<<column<<"on tb:"<<tb_name<<endl;
}

template<typename Type>
void select_value(string &column, string &tb_name, string &cond_column, cond_op_num cond_op, Type cond_value){
    cout<<"select val:"<<column<<" on tb:"<<tb_name<<cond_column<<cond_op<<cond_value<<endl;
}

void delete_value(string &tb_name){
    cout<<"delete tb:"<<tb_name<<endl;
}

template<typename Type>
void delete_value(string &tb_name, string &cond_column, cond_op_num cond_op, Type cond_value){
    cout<<"delete tb:"<<tb_name<<cond_column<<cond_op<<cond_value<<endl;

}

void insert_value(string &tb_name, vector<string> &values){
    cout<<"insert val on table:"<<tb_name<<endl;
    for (auto &val:values) {
        cout<<val<<endl;
    }
}