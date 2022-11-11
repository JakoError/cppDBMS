//
// Created by JakoError on 2022/11/11.
//

#ifndef DBMS_SQLPARSER_HPP
#define DBMS_SQLPARSER_HPP

#include <boost/regex.hpp>

#include <string>
#include <vector>

using boost::regex;

using std::vector;
using std::string;

namespace cppDBMS {
    class SQLParser {
    public:
        enum cmd_num : int {
            create_db = 0, drop_db, use_db, create_tb, drop_tb, select_v, delete_v, insert_v, exit_c
        };

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

        static vector<string> split_by_space(const string &str);
    };
}

#endif //DBMS_SQLPARSER_HPP
