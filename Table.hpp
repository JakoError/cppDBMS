//
// Created by JakoError on 2022/11/10.
//
#ifndef DBMS_TABLE_HPP
#define DBMS_TABLE_HPP

#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <map>

#include "Data.h"

using std::string;
using std::vector;
using std::map;

using size_type = cppDBMS::Data::size_type;

namespace cppDBMS {
    class Table : public Data {
    private:
        void select_preprocess(vector<size_type> &select_line, vector<size_type> &select_col,
                               bool allow_duplicate = true) const;

        bool check_data_same_length() noexcept;

        static string types_to_string(const vector<type_num_type>& types) {
            std::stringstream ss;
            ss << '[';
            for (auto &type: types) {
                if (type == int_type_num)
                    ss << "int";
                else if (type == str_type_num)
                    ss << "string(" + std::to_string(STR_LEN) + ")";
                else
                    ss << "unknown";
            }
            ss << ']';
            return ss.str();
        }

    protected:
        //动态加载数据标识
        bool is_loaded = false;

    public:
        //为内部选取所有行特殊标识
        const static vector<size_type> ALL_LINE;
        //为内部选取所有列特殊标识
        const static vector<size_type> ALL_COL;

        //表名称
        string tb_name;
        //所有列数
        size_type col_length = 0;
        //所有行数
        size_type line_length = 0;
        //表中所有列名列表
        vector<string> column_names;
        //表中所有列类型记录
        vector<type_num_type> column_types;
        //每列按照数据总大小记录
        size_type line_size = 0;
        //每列按照数据大小记录
        vector<size_type> column_offset;

        //列名指向列位置编号
        map<string, size_type> name_to_idx;

        //包含所有int类型的内存数据表，非该类型列index为空
        vector<vector<int>> int_cols;
        //含所有string类型的内存数据表，非该类型列index为空
        vector<vector<string>> str_cols;

        //主键所在的列位置编号，无主键为-1
        size_type primary = -1;

        //索引表
        map<std::size_t,size_type> index;

        Table(const path &dataPath, string tbName) : Data(dataPath), tb_name(std::move(tbName)) {}

        Table(const path &dataPath, string tbName,
              const vector<string> &columnNames, const vector<type_num_type> &columnTypes,
              size_type primary);

        path get_tb_info_path() {
            return get_data_path() / (tb_name + ".tb");
        }

        path get_tb_data_path() {
            return get_data_path() / (tb_name + ".dat");
        }

        path get_tb_idx_path() {
            return get_data_path() / (tb_name + ".idx");
        }
        void build_index();

        void load_data() override;

        void save_data() override;

        void release_data() override;

        void save_data(vector<size_type> select_line, vector<size_type> select_col);

        size_type delete_data();

        size_type delete_data(vector<size_type> select_line);

        size_type insert_data(const vector<string> &value_strs, const vector<type_num_type> &value_types);

        void create() override;

        void drop() override;

        size_type get_column_idx(const string &col_name);

        vector<size_type> cond_on_data(const string &cond_col_name, const boost::function<bool(const char *)> &cond);

        vector<size_type> cond_on_data(const size_type &cond_col, const boost::function<bool(const char *)> &cond);

        static vector<size_type>
        cond_on_data(vector<string> &cond_col, const boost::function<bool(const char *)> &cond) {
            vector<size_type> selected_lines;
            for (size_type i = 0; i < cond_col.size(); ++i) {
                if (cond(cond_col[i].c_str()))
                    selected_lines.push_back(i);
            }
            return selected_lines;
        }

        static vector<size_type> cond_on_data(vector<int> &cond_col, const boost::function<bool(const char *)> &cond) {
            vector<size_type> selected_lines;
            for (size_type i = 0; i < cond_col.size(); ++i) {
                if (cond(reinterpret_cast<const char *>(&cond_col[i])))
                    selected_lines.push_back(i);
            }
            return selected_lines;
        }

        template<typename T>
        bool check_duplicate(const T &value);

        string data_tostring(vector<size_type> select_line = {-1}, vector<size_type> select_col = {-1},
                             const string &seg = "|");

        string data_tostring(const vector<size_type> &select_line,
                             const vector<string> &select_col_name = {"*"},
                             const string &seg = "|");
    };
}
#endif //DBMS_TABLE_HPP
