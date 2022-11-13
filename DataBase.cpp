//
// Created by JakoError on 2022/11/10.
//

#include "DataBase.hpp"

using namespace boost::filesystem;

/**
 * only load not considering delete that not exist
 */
void cppDBMS::DataBase::load_data() {
    for (const auto &entry: directory_iterator(get_data_path())) {
        auto tb_name = entry.path().filename().string();
        if (is_directory(entry) && name_to_table.count(tb_name) == 0) {
            this->tables.emplace_back(entry.path(), tb_name);
        }
    }
    name_to_table.clear();
    for (Table &tb: tables) {
        name_to_table[tb.tb_name] = &tb;
    }
}

bool cppDBMS::DataBase::is_tb_exists(const string &tb_name) {
    load_data();
    return name_to_table.count(tb_name) != 0;
}

cppDBMS::Table *cppDBMS::DataBase::getTable(const string &tb_name) {
    load_data();
    if (name_to_table.count(tb_name) == 0)
        return nullptr;
    return name_to_table[tb_name];
}

void cppDBMS::DataBase::create() {
    if (exists(get_data_path()))
        BOOST_THROW_EXCEPTION(SqlException("Database " + db_name + " already exists!"));
    create_directory(get_data_path());
    if (!exists(get_data_path()))
        BOOST_THROW_EXCEPTION(IOException("create database failed"));
}

void cppDBMS::DataBase::drop() {
    for (auto &tb: tables) {
        tb.drop();
    }
    remove_all(get_data_path());
    if (exists(get_data_path()))
        BOOST_THROW_EXCEPTION(IOException("remove database file failed"));
}

void cppDBMS::DataBase::create_table(const string &tb_name,
                                     const vector<string> &column_names, const vector<type_num_type> &column_types,
                                     int primary) {
    load_data();
    if (is_tb_exists(tb_name))
        BOOST_THROW_EXCEPTION(SqlException("Table " + tb_name + " already exists in Database " + db_name + "!"));
    //add record
    Table &tb = add_table_record({get_data_path() / tb_name, tb_name, column_names, column_types, primary});
    //create files
    tb.create();
    tb.save_data();
}

void cppDBMS::DataBase::drop_table(const string &tb_name) {
    load_data();
    if (!is_tb_exists(tb_name))
        BOOST_THROW_EXCEPTION(SqlException("Table " + tb_name + " not exists in Database " + db_name + "!"));
    Table *tb = getTable(tb_name);
    //drop files
    tb->drop();
    //drop record
    drop_table_record(tb_name);
}

void cppDBMS::DataBase::save_data() {
}

void cppDBMS::DataBase::release_data() {
    for (auto &tb: tables) {
        tb.release_data();
    }
    tables.clear();
    name_to_table.clear();
}
