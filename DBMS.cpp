//
// Created by JakoError on 2022/11/10.
//
#include "DBMS.hpp"

using namespace boost::filesystem;

const string cppDBMS::DBMS::FAIL_MSG = "failed: ";
const string cppDBMS::DBMS::SUCC_MSG = "succeed: ";

bool cppDBMS::DBMS::is_db_exists(const string &db_name) {
    return std::ranges::any_of(databases, [&](const auto &db) { return db.db_name == db_name; });
}

void cppDBMS::DBMS::create() {
    BOOST_THROW_EXCEPTION(std::runtime_error("cannot create DBMS"));
}

void cppDBMS::DBMS::drop() {
    BOOST_THROW_EXCEPTION(std::runtime_error("cannot drop DBMS"));
}

void cppDBMS::DBMS::load_data() {
    for (const auto &entry: directory_iterator(get_data_path())) {
        if (is_directory(entry))
            this->databases.emplace_back(entry.path(), entry.path().filename().string());
    }
}

vector<cppDBMS::DataBase> &cppDBMS::DBMS::getDatabases() {
    load_data();
    return databases;
}

cppDBMS::DataBase *cppDBMS::DBMS::getDatabase(const string &db_name) {
    for (DataBase &db: getDatabases()) {
        if (db.db_name == db_name)
            return &db;
    }
    return nullptr;
}

string cppDBMS::DBMS::create_database(const string &db_name) {
    if (is_db_exists(db_name))
        return FAIL_MSG + "Database " + db_name + " already exists!";
    DataBase(get_data_path() / db_name, db_name).create();
    return SUCC_MSG + "Database " + db_name + " created.";
}

string cppDBMS::DBMS::drop_database(const string &db_name) {
    DataBase *db = getDatabase(db_name);
    if (db == nullptr)
        return FAIL_MSG + "Database " + db_name + " not exists!";
    db->drop();
    //reload data to check existence
    if (is_db_exists(db_name))
        return FAIL_MSG + "Database " + db_name + " failed to drop!";
    return SUCC_MSG + "Database " + db_name + " dropped.";
}

string cppDBMS::DBMS::use_database(const string &db_name) {
    current_database = getDatabase(db_name);
    if (current_database == nullptr)
        return FAIL_MSG + "Database " + db_name + " not exists!";
    else
        return SUCC_MSG + "using Database " + db_name;
}

string cppDBMS::DBMS::create_table(const string &tb_name,
                                   const vector<string> &column_names, const vector<type_num> &column_types,
                                   int primary_index) {
    if (current_database == nullptr)
        return FAIL_MSG + " use Database first!";
    current_database->create_table(tb_name,column_names,column_types,primary_index);

    current_database->load_data();
    if (current_database->is_tb_exists(tb_name))
        return FAIL_MSG + "Table " + tb_name + " already exists in Database " + current_database->db_name + "!";
    Table(current_database->get_data_path())

    if (column_names.size() != column_types.size())
        BOOST_THROW_EXCEPTION(std::runtime_error("column_names and column_types have different size!"));
    std::fstream data;
    //table info write
    data.open(db_path / current_database / tb_name / (tb_name + ".tb"));
//        data.write(reinterpret_cast<const char *>(&size), sizeof(size));
    data << column_types.size();
    for (auto &type: column_types) {
//            data.write(reinterpret_cast<const char *>(&type), sizeof(type));
        data << type;
    }
    //table column name write
    for (auto &name: column_names) {
        data << name.length() << name;
    }
    //table .dat create
    data.open(db_path / current_database / tb_name / (tb_name + ".dat"));
    //table .idx create
    if (primary_index != -1) {
        data.open(db_path / current_database / tb_name / (tb_name + ".idx"));
        data << primary_index;
    }
    return SUCC_MSG + "Table " + tb_name + " created!";
}
