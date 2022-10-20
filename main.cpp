#include <boost/regex.hpp>
#include <iostream>
#include <string>

using std::cout;
using std::cin;
using std::string;

using boost::regex;

const string cmd_line="> ";

const string name_regex="([A-Za-z0-9]{1,63})";

const string column_regex="("+name_regex+" ((int)|(string))( primary)?)";

const string value_regex="((0|[1-9][0-9]*)|(\"[A-Za-z0-9]{0,256}\"))";

const string cond_regex="("+name_regex+" [=<>] "+value_regex+")";

int main(){
    regex re_exit("exit");
    regex re_create_db("create database "+name_regex);
    regex re_drop_db("drop database "+name_regex);
    regex re_use_db("use "+name_regex);
    regex re_create_tb("create table "+name_regex+"\\("+column_regex+"(, "+column_regex+")+ \\)");
//    regex re_create_tb("create table "+name_regex+"\\("
//                       "((\\))|"
//                       "("+column_regex+"\\))|"
//                       "(("+column_regex+", )+("+column_regex+"\\))))");
    regex re_drop_tb("drop table [A-Za-z0-9]{1,63}");
    regex re_select("select "+name_regex+" from "+name_regex+" \\[ where "+cond_regex+" \\]");
    regex re_delete("delete "+name_regex+" \\[ where"+cond_regex+" \\]");
    regex re_insert("insert "+name_regex+" values\\(("+value_regex+"(, "+value_regex+")*)\\)");
    cout<<"(, "+value_regex+")*";
//    while (true){
//        cout<<cmd_line;
//    }
}