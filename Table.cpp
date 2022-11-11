//
// Created by JakoError on 2022/11/10.
//

#include "Table.hpp"

#include <boost/shared_ptr.hpp>
#include <boost/filesystem.hpp>
#include <boost/throw_exception.hpp>
#include <boost/range/algorithm.hpp>

#include <iomanip>

using boost::filesystem::fstream;
using cppDBMS::read_value;

void cppDBMS::Table::load_data() {
    //prepare io
    fstream info;
    fstream data;
    info.open(get_tb_info_path(), std::ios::in);
    data.open(get_tb_data_path(), std::ios::in);
    if (!info.is_open() || !data.is_open())
        BOOST_THROW_EXCEPTION(std::runtime_error("table file io failed!"));

    //info table size
    info >> this->col_length;
    //init container & parameter
    this->column_names = vector<string>(this->col_length);
    this->column_types = vector<type_num_type>(this->col_length);
    this->column_offset = vector<size_type>(this->col_length);
    this->line_size = 0;

    //read column type
    for (size_type i = 0; i < this->col_length; ++i) {
        info >> this->column_types[i];
        this->column_offset[i] = this->line_size;
        if (this->column_types[i] == int_type_num)
            this->line_size += INT_SIZE;
        else
            this->line_size += STR_SIZE;
    }

    //read column name
    for (size_type i = 0; i < this->col_length; ++i) {
        size_t length;
        data >> length;
        boost::shared_ptr<char> buffer(new char[length]);
        data.read(buffer.get(), static_cast<std::streamsize>(length));
        this->column_names[i] = string(buffer.get(), length);
    }

    //read length of data lines
    this->line_length = static_cast<size_type>(boost::filesystem::file_size(get_tb_data_path()) / line_size);

    //flag
    this->is_loaded = true;
}

size_type cppDBMS::Table::get_column_idx(const string &col_name) {
    for (size_type i = 0; i < this->col_length; ++i) {
        if (this->column_names[i] == col_name)
            return i;
    }
    return -1;
}

template<typename T>
vector<T> cppDBMS::Table::load_column_data(const string &col_name) {
    //check
    if (!is_loaded)
        load_data();
    //get column index
    auto col_idx = get_column_idx(col_name);

    if (get_type_num<T>() != column_types[col_idx])
        BOOST_THROW_EXCEPTION(std::runtime_error("Type of column not match"));

    //prepare io
    fstream data;
    data.open(get_data_path(), std::ios::in);

    //init container
    vector<T> record;

    //read record
    size_type line_idx = 0;
    while (!data.eof()) {
        data.seekg(static_cast<std::istream::off_type>(column_offset[col_idx]),
                   static_cast<std::ios_base::seekdir>(line_idx * line_size));
        if (data.eof())
            break;
        T value;
        read_value(data, value);
        record.push_back(value);
    }

    return record;
}

string cppDBMS::Table::data_tostring(vector<size_type> select_line = {-1},
                                     vector<size_type> select_col = {-1}, const string &seg = "|") {
    //check select
    if (select_line == vector{-1}) {
        select_line = vector<size_type>(this->line_length);
        for (size_type i = 0; i < this->line_length; ++i) {
            select_line[i] = i;
        }
    } else {
        boost::sort(select_line);
    }
    if (select_col == vector{-1}) {
        select_col = vector<size_type>(this->col_length);
        for (size_type i = 0; i < this->col_length; ++i) {
            select_line[i] = i;
        }
    } else {
        boost::sort(select_col);
    }

    //prepare io
    fstream data;
    data.open(get_data_path(), std::ios::in);

    vector<vector<string>> str_container(select_line.size(), vector<string>(select_col.size()));
    //keep space length ensure align of each column
    vector<string::size_type> max_length(select_col.size());

    for (size_type i = 0; i < select_col.size(); ++i) {
        max_length[i] = column_names[select_col[i]].length();
    }
    //read & to string
    for (size_type i = 0; i < select_line.size(); ++i) {
        for (size_type j = 0; j < select_col.size(); ++j) {
            data.seekg(static_cast<std::istream::off_type>(column_offset[select_col[j]]),
                       static_cast<std::ios_base::seekdir>(select_line[i] * line_size));
            if (data.eof())
                BOOST_THROW_EXCEPTION(std::runtime_error("read data out of range"
                                                         "( line= " + std::to_string(select_line[i]) +
                                                         " col= " + std::to_string(select_col[j]) + " )"));
            //recorde string
            if (column_types[select_col[j]] == int_type_num) {
                int value;
                read_value(data, value);
                str_container[i][j] = std::to_string(value);
            } else {
                string value;
                read_value(data, value);
                str_container[i][j] = value;
            }
            //recorde string len
            if (str_container[i][j].length() > max_length[i])
                max_length[i] = str_container[i][j].length();
        }
    }

    //string with align
    std::stringstream ss;
    for (size_type i = 0; i < select_line.size(); ++i) {
        ss << std::setfill(' ') << std::setw(static_cast<std::streamsize>(max_length[i])) << column_names[i] << seg;
    }
    ss << std::endl;
    for (size_type i = 0; i < select_line.size(); ++i) {
        for (size_type j = 0; j < select_col.size(); ++j) {
            ss << std::setfill(' ') << std::setw(static_cast<std::streamsize>(max_length[i])) << str_container[i][j]
               << seg;
        }
    }
    ss << std::endl;

    return ss.str();
}

void cppDBMS::Table::create() {

}

void cppDBMS::Table::drop() {

}


