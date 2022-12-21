//
// Created by JakoError on 2022/11/10.
//
#ifndef DBMS_DATA_H
#define DBMS_DATA_H

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/shared_ptr.hpp>

#include <utility>

#include "DBMSExceptions.hpp"

#ifndef STR_LENGTH
#define STR_LENGTH 256
#endif //STR_LENGTH

#ifndef DATA_SIZE_TYPE
#define DATA_SIZE_TYPE int
#endif //DATA_SIZE_TYPE

using std::string;

using boost::filesystem::fstream;
using boost::filesystem::path;

namespace cppDBMS {
    class Data {
    public:
        typedef DATA_SIZE_TYPE size_type;
        typedef int type_num_type;

        path dataPath;

        static const int STR_LEN = STR_LENGTH;
        static const size_t STR_SIZE = sizeof(char) * STR_LEN;
        static const size_t INT_SIZE = sizeof(int);

        enum type_num : type_num_type {
            int_type_num = 0, str_type_num = 1
        };

        template<typename T>
        type_num_type get_type_num() {
            if (std::is_same<T, int>::value)
                return int_type_num;
            if (std::is_same<T, std::string>::value)
                return str_type_num;
            return -1;
        }

        virtual path get_data_path() {
            return dataPath;
        }

        virtual void create() = 0;

        virtual void drop() = 0;

        virtual void load_data() = 0;

        virtual void save_data() = 0;

        virtual void release_data() = 0;

        explicit Data(path dataPath) : dataPath(std::move(dataPath)) {}
    };

    static void read_value(fstream &f, int &value) {
        f.read(reinterpret_cast<char *>(&value), sizeof(int));
    }

    static void read_value(fstream &f, string &value) {
        char buff[STR_LENGTH];
        f.read(buff, STR_LENGTH);
        string take(buff, STR_LENGTH);
        boost::trim(take);
        value = take;
    }

    static void write_value(fstream &f, int value) {
        f.write(reinterpret_cast<char *>(&value), sizeof(int));
    }

    static void write_value(fstream &f, string &value) {
        if (value.length() > Data::STR_LEN)
            BOOST_THROW_EXCEPTION(std::runtime_error("string max length " + std::to_string(Data::STR_LEN)));
        f.write(value.c_str(), static_cast<std::streamsize>(value.length()));
        string space(Data::STR_LEN - value.length(), ' ');
        f.write(space.c_str(), static_cast<std::streamsize>(space.length()));
    }

    static string read(fstream &f, string::size_type len) {
        boost::shared_ptr<char> buffer(new char[len]);
        f.read(buffer.get(), static_cast<std::streamsize>(len));
        return {buffer.get(), len};
    }

    template<typename T>
    static T read(fstream &f) {
        T v;
        f.read(reinterpret_cast<char *>(&v), sizeof(T));
        return v;
    }

    static void write(fstream &f, string &value) {
        f.write(value.c_str(), static_cast<std::streamsize>(value.length()));
    }

    template<typename T>
    static void write(fstream &f, T value) {
        f.write(reinterpret_cast<char *>(&value), sizeof(T));
    }

    template<typename T>
    static size_t hash(T value) {
        std::hash<T> T_hash;
        return T_hash(value);
    }

}

#endif //DBMS_DATA_H
