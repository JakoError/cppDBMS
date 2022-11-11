//
// Created by JakoError on 2022/11/11.
//
#include "SQLParser.hpp"

#include <boost/algorithm/string.hpp>

string cppDBMS::SQLParser::seg = "[\\s\t]+";
string cppDBMS::SQLParser::seg_v = "[\\s\t]*";
string cppDBMS::SQLParser::name_regex = "([a-z]{1,63})";
string cppDBMS::SQLParser::column_regex = "(" + name_regex + seg + "((int)|(string))(" + seg + "primary)?)";
string cppDBMS::SQLParser::value_regex = "((0|[1-9][0-9]*)|(\".{0,256}\"))";
string cppDBMS::SQLParser::cond_regex = "(" + name_regex + seg_v + "[=<>]" + seg_v + value_regex + ")";
string cppDBMS::SQLParser::where_regex = "(" + seg + "where" + seg + cond_regex + ")";

regex cppDBMS::SQLParser::re_create_db = regex("create" + seg + "database" + seg + name_regex);
regex cppDBMS::SQLParser::re_drop_db = regex("drop" + seg + "database" + seg + name_regex);
regex cppDBMS::SQLParser::re_use_db = regex("use" + seg + name_regex);
regex cppDBMS::SQLParser::re_create_tb = regex("create" + seg + "table" + seg + name_regex + seg_v +
                                 "\\(" + seg_v + column_regex + "(" + seg_v + "," + seg_v + column_regex + ")+" +
                                 seg_v + "\\)");
regex cppDBMS::SQLParser::re_drop_tb = regex("drop" + seg + "table" + seg + name_regex);
regex cppDBMS::SQLParser::re_select = regex("select" + seg + "(" + name_regex + "|[*])" + seg + "from" + seg + name_regex
                              + where_regex + "?");
regex cppDBMS::SQLParser::re_delete = regex("delete" + seg + name_regex + where_regex + "?");
regex cppDBMS::SQLParser::re_insert = regex("insert" + seg + name_regex + seg + "values" + seg_v +
                              "\\(" + seg_v + "(" + value_regex + seg_v + "(," + seg_v + value_regex + seg_v + ")*)" +
                              "\\)");
regex cppDBMS::SQLParser::re_exit = regex("exit");

vector<string> cppDBMS::SQLParser::split_by_space(const string &str) {
    std::vector<string> strs;
    boost::split(strs, str, boost::is_any_of(" \t"), boost::token_compress_on);
    return strs;
}
