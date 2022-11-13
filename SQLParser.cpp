//
// Created by JakoError on 2022/11/11.
//
#include "SQLParser.hpp"

#include <boost/algorithm/string.hpp>

string cppDBMS::SQLParser::seg = "[\\s\t]+";
string cppDBMS::SQLParser::seg_v = "[\\s\t]*";
string cppDBMS::SQLParser::name_regex = "([a-z]{1,63})";
string cppDBMS::SQLParser::names_regex = "(" + name_regex + "(" + seg_v + "," + seg_v + name_regex + ")?)";
string cppDBMS::SQLParser::column_regex = "(" + name_regex + seg + "((int)|(string))(" + seg + "primary)?)";
string cppDBMS::SQLParser::value_regex = "((0|[1-9][0-9]*)|(\".{0,256}\"))";
string cppDBMS::SQLParser::cond_regex = "(" + name_regex + seg + "[=<>]" + seg + value_regex + ")";
string cppDBMS::SQLParser::where_regex = "(" + seg + "where" + seg + cond_regex + ")";

regex cppDBMS::SQLParser::re_create_db = regex("create" + seg + "database" + seg + name_regex);
regex cppDBMS::SQLParser::re_drop_db = regex("drop" + seg + "database" + seg + name_regex);
regex cppDBMS::SQLParser::re_use_db = regex("use" + seg + name_regex);
regex cppDBMS::SQLParser::re_create_tb = regex("create" + seg + "table" + seg + name_regex + seg_v +
                                               "\\(" + seg_v + column_regex + "(" + seg_v + "," + seg_v + column_regex +
                                               ")+" +
                                               seg_v + "\\)");
regex cppDBMS::SQLParser::re_drop_tb = regex("drop" + seg + "table" + seg + name_regex);
regex cppDBMS::SQLParser::re_select = regex(
        "select" + seg + "(" + names_regex + "|[*])" + seg + "from" + seg + name_regex
        + where_regex + "?");
//regex cppDBMS::SQLParser::re_select = regex("select" + seg + "(" + name_regex + "|[*])" + seg + "from" + seg + name_regex
//                              + where_regex + "?");
regex cppDBMS::SQLParser::re_delete = regex("delete" + seg + name_regex + where_regex + "?");
regex cppDBMS::SQLParser::re_insert = regex("insert" + seg + name_regex + seg + "values" + seg_v +
                                            "\\(" + seg_v + "(" + value_regex + seg_v + "(," + seg_v + value_regex +
                                            seg_v + ")*)" +
                                            "\\)");
regex cppDBMS::SQLParser::re_exit = regex("exit");

vector<string> cppDBMS::SQLParser::split_by_space(const string &str) {
    std::vector<string> strs;
    boost::split(strs, str, boost::is_any_of(" \t"), boost::token_compress_on);
    return strs;
}

vector<string> cppDBMS::SQLParser::extract_columns(string &str) {
    auto l = str.find_first_of(',');
    auto r = str.find_last_of(',');

    if (l == string::npos || r == string::npos)
        return {};

    //l move backward
    l--;
    while (str[l] == ' ' || str[l] == '\t')//eat spaces before ,
        l--;
    while (str[l] != ' ' || str[l] != '\t')//eat first column
        l--;
    l++;//offset to first one char

    //r move forward
    r++;
    while (str[r] == ' ' || str[r] == '\t')//eat after before ,
        r++;
    while (str[r] != ' ' || str[r] != '\t')//eat last column
        r++;

    string columns_str = str.substr(l, r - l);

    vector<string> columns_strs;
    boost::split(columns_strs, columns_str, boost::is_any_of(" \t,"), boost::token_compress_on);
    str.erase(str.begin() + static_cast<string::difference_type>(l),
              str.begin() + static_cast<string::difference_type>(r));

    return columns_strs;
}

