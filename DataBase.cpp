//
// Created by JakoError on 2022/11/10.
//

#include "DataBase.hpp"

using namespace boost::filesystem;

void cppDBMS::DataBase::load_data() {
    for (const auto &entry: directory_iterator(get_data_path())) {
        if (is_directory(entry))
            this->tables.emplace_back(entry.path(), entry.path().filename().string());
    }
}

bool cppDBMS::DataBase::is_tb_exists(const string &tb_name) {
    return std::ranges::any_of(tables, [&](const Table &tb) { return tb.tb_name == tb_name; });
}

cppDBMS::Table *cppDBMS::DataBase::getTable(const string &tb_name) {
    for (Table &tb: tables) {
        if (tb.tb_name == tb_name)
            return &tb;
    }
    return nullptr;
}

void cppDBMS::DataBase::create() {
    fstream db_dir;
    db_dir.open(get_data_path());
    if (!db_dir.is_open())
        BOOST_THROW_EXCEPTION(std::runtime_error("create database failed"));
}

void cppDBMS::DataBase::drop() {
    remove_all(get_data_path());
    if (exists(get_data_path()))
        BOOST_THROW_EXCEPTION(std::runtime_error("remove database file failed"));
}

void cppDBMS::DataBase::create_table(const string &tb_name,
                                     const vector<string> &column_names, const vector<type_num> &column_types,
                                     int primary_index) {
    load_data();
    if(is_tb_exists(tb_name))
        BOOST_THROW_EXCEPTION(std::runtime_error("Table " + tb_name + " already exists in Database " + db_name + "!"))
}
