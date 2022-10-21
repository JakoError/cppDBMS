#include <boost/regex.hpp>
#include <boost/algorithm/string.hpp>
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

string cmd_line="> ";

string seg="[\\s]+";
string seg_v="[\\s]*";
string name_regex="([A-Za-z0-9]{1,63})";
string column_regex="("+name_regex+" ((int)|(string))( primary)?)";
string value_regex="((0|[1-9][0-9]*)|(\"[A-Za-z0-9]{0,256}\"))";
string cond_regex="("+name_regex+seg_v+"[=<>]"+seg_v+value_regex+")";

enum cmd_num{create_db=0,drop_db,use_db,create_tb,drop_tb,select_v,delete_v,insert_v};

int main(){
    regex re_create_db("create"+seg+"database"+seg+name_regex);
    regex re_drop_db("drop"+seg+"database"+seg+name_regex);
    regex re_use_db("use"+seg+name_regex);
    regex re_create_tb("create"+seg+"table"+seg+name_regex+seg_v+"\\("+seg_v+column_regex+"("+seg+","+seg+column_regex+")+\t\\)");
    regex re_drop_tb("drop \ttable \t[A-Za-z0-9]{1,63}");
    regex re_select("select \t("+name_regex+"|[*]) \tfrom \t"+name_regex+" \t(where \t"+cond_regex+")?");
    regex re_delete("delete \t"+name_regex+" \t(where \t"+cond_regex+")?");
    regex re_insert("insert \t"+name_regex+" \tvalues\\(("+value_regex+"(\t,\t"+value_regex+")*)\t\\)");
    regex re_exit("exit");
    while (true){
        cout<<cmd_line;
        string cmd;
        getline(cin,cmd);
        trim(cmd);

        if (regex_match(cmd,re_create_db)){
            vector<string> strs;
            boost::split(strs,cmd,boost::is_any_of(" \t"));
            for (auto &str:strs) {
                cout<<str<<endl;
            }
        }else if (regex_match(cmd,re_drop_db)){
            continue;
        }else if (regex_match(cmd,re_use_db)){
            continue;
        }else if (regex_match(cmd,re_create_tb)){
            continue;
        }else if (regex_match(cmd,re_drop_tb)){
            continue;
        }else if (regex_match(cmd,re_select)){
            continue;
        }else if (regex_match(cmd,re_delete)){
            continue;
        }else if (regex_match(cmd,re_insert)){
            continue;
        }else if (regex_match(cmd,re_exit)){
            exit(0);
        }else{
            std::flush(cout);
            cout<<"Syntax Wrong!"<<endl;
            std::flush(cout);
        }
    }
}