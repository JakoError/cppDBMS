//
// Created by JakoError on 2022/11/10.
//
#ifndef DBMS_DATA_H
#define DBMS_DATA_H

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include <utility>

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

        explicit Data(path dataPath) : dataPath(std::move(dataPath)) {}
    };

    static void read_value(std::istream &is, int &value) {
        is >> value;
    }

    static void read_value(std::istream &is, string &value) {
        char buff[STR_LENGTH];
        is.read(buff, STR_LENGTH);
        string take(buff, STR_LENGTH);
        boost::trim(take);
        value = take;
    }
}

#endif //DBMS_DATA_H
