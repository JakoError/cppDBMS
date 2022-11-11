//
// Created by JakoError on 2022/11/10.
//

#ifndef DBMS_TABLE_HPP
#define DBMS_TABLE_HPP

#include <string>
#include <utility>
#include <vector>

#include "Data.h"

using std::string;
using std::vector;

using size_type = cppDBMS::Data::size_type;

namespace cppDBMS {
    class Table : public Data {
    protected:
        bool is_loaded = false;

    public:
        string tb_name;
        size_type col_length = 0;
        size_type line_length = 0;
        vector <string> column_names;
        vector <type_num_type> column_types;
        size_type line_size = 0;
        vector <size_type> column_offset;

        Table(const path &dataPath, string tbName) : Data(dataPath), tb_name(std::move(tbName)) {}

        path get_tb_info_path() {
            return get_data_path() / (tb_name + ".tb");
        }

        path get_tb_data_path() {
            return get_data_path() / (tb_name + ".dat");
        }

        void load_data() override;

        void create() override;

        void drop() override;

        size_type get_column_idx(const string &col_name);

        template<typename T>
        vector <T> load_column_data(const string &col_name);

        string data_tostring(vector <size_type> select_line, vector <size_type> select_col, const string &seg);
    };
}
#endif //DBMS_TABLE_HPP
