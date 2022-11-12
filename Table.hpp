//
// Created by JakoError on 2022/11/10.
//
#ifndef DBMS_TABLE_HPP
#define DBMS_TABLE_HPP

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
        void select_preprocess(vector <size_type> &select_line, vector <size_type> &select_col,
                               bool allow_duplicate = true) const;

    protected:
        bool is_loaded = false;

    public:
        const static vector <size_type> ALL_LINE;
        const static vector <size_type> ALL_COL;

        string tb_name;
        size_type col_length = 0;
        size_type line_length = 0;
        vector <string> column_names;
        vector <type_num_type> column_types;
        size_type line_size = 0;
        vector <size_type> column_offset;

        map <string, size_type> name_to_idx;

        vector <vector<int>> int_cols;
        vector <vector<string>> str_cols;

        size_type primary = -1;

        Table(const path &dataPath, string tbName) : Data(dataPath), tb_name(std::move(tbName)) {}

        Table(const path &dataPath, string tbName,
              const vector <string> &columnNames, const vector <type_num_type> &columnTypes,
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

        void load_data() override;

        void save_data() override;

        void release_data() override;

        void save_data(vector <size_type> select_line, vector <size_type> select_col);

        void create() override;

        void drop() override;

        size_type get_column_idx(const string &col_name);

        vector <size_type> cond_on_data(const string &cond_col_name, const boost::function<bool(const char*)>& cond);

        vector <size_type> cond_on_data(const size_type &cond_col, const boost::function<bool(const char*)>& cond);

        static vector <size_type> cond_on_data(vector<string> &cond_col, const boost::function<bool(const char*)>& cond) {
            vector<size_type> selected_lines;
            for (size_type i = 0; i < cond_col.size(); ++i) {
                if (cond(cond_col[i].c_str()))
                    selected_lines.push_back(i);
            }
            return selected_lines;
        }

        static vector <size_type> cond_on_data(vector<int> &cond_col, const boost::function<bool(const char*)>& cond) {
            vector<size_type> selected_lines;
            for (size_type i = 0; i < cond_col.size(); ++i) {
                if (cond(reinterpret_cast<const char *>(&cond_col[i])))
                    selected_lines.push_back(i);
            }
            return selected_lines;
        }


        string data_tostring(vector <size_type> select_line = {-1}, vector <size_type> select_col = {-1},
                             const string &seg = "|");

        string data_tostring(const vector <size_type> &select_line,
                             const vector <string> &select_col_name = {"*"},
                             const string &seg = "|");
    };
}
#endif //DBMS_TABLE_HPP
