//
// Created by JakoError on 2022/11/11.
//
#include "SQLParser.hpp"

#include <boost/algorithm/string.hpp>

//包含1个以上的空字符，作为命令关键字之间的间隔标识
string cppDBMS::SQLParser::seg = "[\\s\t]+";
//包含0个以上的空字符，作为括号之间可省略的间隔标识
string cppDBMS::SQLParser::seg_v = "[\\s\t]*";
//包含1-63位小写字母构成的名称标识
string cppDBMS::SQLParser::name_regex = "([a-z]{1,63})";
//包含了由逗号所隔开的名字列表，用于select语句
string cppDBMS::SQLParser::names_regex = "(" + name_regex + "(" + seg_v + "," + seg_v + name_regex + ")*)";
//包含了由列名，列类型与主键所构成的列信息列表，用于表创建语句s
string cppDBMS::SQLParser::column_regex = "(" + name_regex + seg + "((int)|(string))(" + seg + "primary)?)";
//用于定义条件语句中的数值
string cppDBMS::SQLParser::value_regex = "((0|[1-9][0-9]*)|(\".{0,256}\"))";
//用于定义一个条件语句
string cppDBMS::SQLParser::cond_regex = "(" + name_regex + seg + "[=<>]" + seg + value_regex + ")";
//定义一个where语句作为SQL中的判定
string cppDBMS::SQLParser::where_regex = "(" + seg + "where" + seg + cond_regex + ")";

//创建数据库正则表达式
regex cppDBMS::SQLParser::re_create_db = regex("create" + seg + "database" + seg + name_regex);
//删除数据库正则表达式
regex cppDBMS::SQLParser::re_drop_db = regex("drop" + seg + "database" + seg + name_regex);
//切换数据库正则表达式
regex cppDBMS::SQLParser::re_use_db = regex("use" + seg + name_regex);
//创建表正则表达式
regex cppDBMS::SQLParser::re_create_tb = regex("create" + seg + "table" + seg + name_regex + seg_v +
                                               "\\(" + seg_v + column_regex + "(" + seg_v + "," + seg_v + column_regex +
                                               ")+" +
                                               seg_v + "\\)");
//删除表正则表达式
regex cppDBMS::SQLParser::re_drop_tb = regex("drop" + seg + "table" + seg + name_regex);
//Select筛选记录语句正则表达式
regex cppDBMS::SQLParser::re_select = regex(
        "select" + seg + "(" + names_regex + "|[*])" + seg + "from" + seg + name_regex
        + where_regex + "?");
//regex cppDBMS::SQLParser::re_select = regex("select" + seg + "(" + name_regex + "|[*])" + seg + "from" + seg + name_regex
//                              + where_regex + "?");
//Delete删除筛选语句正则表达式
regex cppDBMS::SQLParser::re_delete = regex("delete" + seg + name_regex + where_regex + "?");
//Insert语句正则表达式
regex cppDBMS::SQLParser::re_insert = regex("insert" + seg + name_regex + seg + "values" + seg_v +
                                            "\\(" + seg_v + "(" + value_regex + seg_v + "(," + seg_v + value_regex +
                                            seg_v + ")*)" +
                                            "\\)");
regex cppDBMS::SQLParser::re_exit = regex("exit");

/**
 * 按照空格分割字符串
 * @param str
 * @return 分割后的字符串列表
 */
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
    while (str[l] != ' ' && str[l] != '\t')//eat first column
        l--;
    l++;//offset to first one char

    //r move forward
    r++;
    while (str[r] == ' ' || str[r] == '\t')//eat after before ,
        r++;
    while (str[r] != ' ' && str[r] != '\t')//eat last column
        r++;

    string columns_str = str.substr(l, r - l);

    vector<string> columns_strs;
    boost::split(columns_strs, columns_str, boost::is_any_of(" \t,"), boost::token_compress_on);
    str.erase(str.begin() + static_cast<string::difference_type>(l),
              str.begin() + static_cast<string::difference_type>(r));

    return columns_strs;
}

