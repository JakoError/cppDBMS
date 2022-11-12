//
// Created by JakoError on 2022/11/10.
//

#include "Table.hpp"

#include <boost/shared_ptr.hpp>
#include <boost/filesystem.hpp>
#include <boost/throw_exception.hpp>
#include <boost/range/algorithm.hpp>

#include <iomanip>
#include <utility>

using boost::filesystem::fstream;
using cppDBMS::read_value;

const vector<size_type> cppDBMS::Table::ALL_LINE = {-1};
const vector<size_type> cppDBMS::Table::ALL_COL = {-1};

void cppDBMS::Table::load_data() {
    if (is_loaded)
        return;

    //prepare io
    fstream info;
    fstream data;
    info.open(get_tb_info_path(), std::ios::in | std::ios::binary);
    data.open(get_tb_data_path(), std::ios::in | std::ios::binary);
    if (!info || !data)
        BOOST_THROW_EXCEPTION(IOException("table file io failed!"));

    //info table size
    this->col_length = read<size_type>(info);
    //init container & parameter
    this->column_names = vector<string>(this->col_length);
    this->column_types = vector<type_num_type>(this->col_length);
    this->column_offset = vector<size_type>(this->col_length);
    this->line_size = 0;

    //read column type
    for (size_type i = 0; i < this->col_length; ++i) {
        this->column_types[i] = read<type_num_type>(info);
//        info >> this->column_types[i];
        this->column_offset[i] = this->line_size;
        if (this->column_types[i] == int_type_num)
            this->line_size += INT_SIZE;
        else
            this->line_size += STR_SIZE;
    }

    //read column name
    for (size_type i = 0; i < this->col_length; ++i) {
        size_t length;
        length = read<size_t>(info);
        this->column_names[i] = read(info, length);
    }

    //create name to idx mapper
    this->name_to_idx.clear();
    for (size_type i = 0; i < this->col_length; ++i) {
        this->name_to_idx[column_names[i]] = i;
    }

    //read length of data lines
    this->line_length = static_cast<size_type>(boost::filesystem::file_size(get_tb_data_path()) / line_size);

    //load columns to vectors
    this->int_cols = vector<vector<int>>(this->col_length);
    this->str_cols = vector<vector<string>>(this->col_length);
    for (size_type j = 0; j < this->col_length; ++j) {
        if (column_types[j] == int_type_num) {
            this->int_cols[j] = vector<int>(this->line_length);
        } else {
            this->str_cols[j] = vector<string>(this->line_length);
        }
    }

    for (size_type i = 0; i < this->line_length; ++i) {
        for (size_type j = 0; j < this->col_length; ++j) {
            data.clear();
            data.seekg(i * line_size + column_offset[j],
                       std::ios::beg);
            if (data.eof())
                BOOST_THROW_EXCEPTION(IOException("read data out of range"
                                                  "( line= " + std::to_string(i) +
                                                  " col= " + std::to_string(j) + " )"));
            if (column_types[j] == int_type_num) {
                int value;
                read_value(data, value);
                this->int_cols[j][i] = value;
            } else {
                string value;
                read_value(data, value);
                this->str_cols[j][i] = value;
            }
        }
    }

    //flag
    this->is_loaded = true;
}

void cppDBMS::Table::save_data() {
    save_data({-1}, {-1});
}

void cppDBMS::Table::save_data(vector<size_type> select_line,
                               vector<size_type> select_col) {
    //prepare io
    fstream info;
    fstream data;
    info.open(get_tb_info_path(), std::ios::out | std::ios::binary);
    data.open(get_tb_data_path(), std::ios::out | std::ios::binary);
    if (!info || !data)
        BOOST_THROW_EXCEPTION(IOException("table file io failed!"));

    //save info
    //-info table size
    write(info, this->col_length);

    //-write column type
    for (size_type i = 0; i < this->col_length; ++i) {
        write(info, this->column_types[i]);
    }

    //-write column name
    for (size_type i = 0; i < this->col_length; ++i) {
        write(info, this->column_names[i].length());
        write(info, this->column_names[i]);
    }

    //save data
    //-preprocess selector (require no duplication in save data)
    select_preprocess(select_line, select_col, false);

    //-write vector record to file
    for (size_type i = 0; i < this->line_length; ++i) {
        for (size_type j = 0; j < this->col_length; ++j) {
            if (column_types[j] == int_type_num) {
                write_value(data, int_cols[j][i]);
            } else {
                write_value(data, str_cols[j][i]);
            }
        }
    }
}

size_type cppDBMS::Table::get_column_idx(const string &col_name) {
    if (name_to_idx.count(col_name) == 0)
        return -1;
    return name_to_idx[col_name];
}

string cppDBMS::Table::data_tostring(const vector<size_type> &select_line,
                                     const vector<string> &select_col_name, const string &seg) {
    if (select_col_name == vector<string>{"*"})
        return data_tostring(select_line, ALL_COL, seg);
    vector<size_type> select_col(select_col_name.size());
    for (size_type i = 0; i < select_col_name.size(); ++i) {
        select_col[i] = get_column_idx(select_col_name[i]);
    }
    return data_tostring(select_line, select_col, seg);
}

string cppDBMS::Table::data_tostring(vector<size_type> select_line,
                                     vector<size_type> select_col, const string &seg) {
    //preprocess selector
    select_preprocess(select_line, select_col);

    vector<vector<string>> str_container(select_line.size(), vector<string>(select_col.size()));
    //keep space length ensure align of each column
    vector<string::size_type> max_length(select_col.size());

    for (size_type i = 0; i < select_col.size(); ++i) {
        max_length[i] = column_names[select_col[i]].length();
    }
    //read & to string
    for (size_type j = 0; j < select_col.size(); ++j) {
        if (column_types[select_col[j]] == int_type_num) {
            for (size_type i = 0; i < select_line.size(); ++i) {
                str_container[i][j] = std::to_string(int_cols[j][i]);
            }
        } else {
            for (size_type i = 0; i < select_line.size(); ++i) {
                str_container[i][j] = str_cols[j][i];
            }
        }
    }
    for (size_type j = 0; j < select_col.size(); ++j) {
        for (size_type i = 0; i < select_line.size(); ++i) {
            //recorde string len
            if (str_container[i][j].length() > max_length[i])
                max_length[i] = str_container[i][j].length();
        }
    }

    //string with align
    std::stringstream ss;
    for (size_type i = 0; i < select_line.size(); ++i) {
        ss << std::setw(static_cast<std::streamsize>(max_length[i])) << std::setfill(' ') << column_names[i];
        ss << seg;
    }
    ss << std::endl;
    for (size_type i = 0; i < select_line.size(); ++i) {
        for (size_type j = 0; j < select_col.size(); ++j) {
            ss << std::setw(static_cast<std::streamsize>(max_length[j])) << std::setfill(' ') << str_container[i][j];
            ss << std::setw(0) << seg;
        }
        ss << std::endl;
    }
    ss << std::endl;

    return ss.str();
}

void cppDBMS::Table::select_preprocess(vector<size_type> &select_line, vector<size_type> &select_col,
                                       bool allow_duplicate) const {
    //check select
    if (select_line == vector{-1}) {
        select_line = std::vector<size_type>(line_length);
        for (size_type i = 0; i < line_length; ++i) {
            select_line[i] = i;
        }
    } else {
        boost::sort(select_line);
    }
    if (select_col == vector{-1}) {
        select_col = std::vector<size_type>(col_length);
        for (size_type i = 0; i < col_length; ++i) {
            select_col[i] = i;
        }
    } else {
        boost::sort(select_col);
    }

    for (auto &line: select_line) {
        if (line < 0 || line > line_length)
            BOOST_THROW_EXCEPTION(
                    std::out_of_range("select line " + std::to_string(line) + " out of rage on Table " + tb_name));
    }
    for (auto &col: select_col) {
        if (col < 0 || col > line_length)
            BOOST_THROW_EXCEPTION(
                    std::out_of_range("select column " + std::to_string(col) + " out of rage on Table " + tb_name));
    }

    if (!allow_duplicate) {
        for (auto iter = select_line.begin() + 1; iter < select_line.end(); ++iter) {
            if (*iter == *(iter - 1))
                BOOST_THROW_EXCEPTION(SqlException("not allow duplicate line"));
        }
        for (auto iter = select_col.begin() + 1; iter < select_col.end(); ++iter) {
            if (*iter == *(iter - 1))
                BOOST_THROW_EXCEPTION(SqlException("not allow duplicate column"));
        }
    }

}

void cppDBMS::Table::create() {
    if (!boost::filesystem::exists(get_data_path()))
        boost::filesystem::create_directory(get_data_path());
    if (!boost::filesystem::exists(get_tb_info_path()))
        boost::filesystem::fstream().open(get_tb_info_path(), std::ios::out | std::ios::binary);
    if (!boost::filesystem::exists(get_tb_data_path()))
        boost::filesystem::fstream().open(get_tb_data_path(), std::ios::out | std::ios::binary);
    if (!boost::filesystem::exists(get_tb_idx_path()))
        boost::filesystem::fstream().open(get_tb_idx_path(), std::ios::out | std::ios::binary);
}

void cppDBMS::Table::drop() {
    boost::filesystem::remove(get_tb_info_path());
    boost::filesystem::remove(get_tb_data_path());
    boost::filesystem::remove(get_tb_idx_path());
    boost::filesystem::remove(get_data_path());
    std::stringstream ss;
    if (boost::filesystem::exists(get_tb_info_path()))
        ss << "delete Table " << tb_name << " info file failed ";
    if (boost::filesystem::exists(get_tb_data_path()))
        ss << "delete Table " << tb_name << " data file failed ";

}

cppDBMS::Table::Table(const path &dataPath, string tbName,
                      const vector<string> &columnNames, const vector<type_num_type> &columnTypes,
                      size_type primary) : Data(dataPath), tb_name(std::move(tbName)),
                                           col_length(static_cast<size_type>(columnNames.size())),
                                           column_names(columnNames),
                                           column_types(columnTypes),
                                           primary(primary) {
    if (columnNames.size() != columnTypes.size())
        BOOST_THROW_EXCEPTION(
                SystemException("column names vector length should equal to column types vector length"));
    this->column_offset = vector<size_type>(this->col_length);
    this->line_size = 0;

    for (size_type i = 0; i < this->col_length; ++i) {
        this->column_offset[i] = this->line_size;
        if (this->column_types[i] == int_type_num)
            this->line_size += INT_SIZE;
        else
            this->line_size += STR_SIZE;
    }
}

void cppDBMS::Table::release_data() {
    this->int_cols.clear();
    this->str_cols.clear();
    this->is_loaded = false;
}

vector<size_type> cppDBMS::Table::cond_on_data(const string &cond_col_name, const boost::function<bool(const char *)> &cond) {
    return cond_on_data(get_column_idx(cond_col_name), cond);
}

vector<size_type> cppDBMS::Table::cond_on_data(const size_type &cond_col, const boost::function<bool(const char *)> &cond) {
    if (column_types[cond_col] == int_type_num)
        return Table::cond_on_data(int_cols[cond_col], cond);
    else
        return Table::cond_on_data(str_cols[cond_col], cond);
}
