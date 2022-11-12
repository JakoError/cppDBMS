//
// Created by JakoError on 2022/11/12.
//
#include "Table.hpp"

#include <iostream>

using namespace cppDBMS;

int main() {
    Table tb(R"(D:\Codes\C++\cppDBMS\data\test_tb\)", "test_tb");
    tb.col_length = 2;
    tb.line_length = 2;
    tb.column_types = {Data::int_type_num, Data::str_type_num};
    tb.column_names = {"id", "name"};
    tb.line_size = Data::STR_SIZE;
    tb.str_cols = {{},
                   {"jako", "error"}};
    tb.int_cols = {{1, 222220022},
                   {}};
    tb.create();
    tb.save_data();

    tb.load_data();
    std::cout << tb.data_tostring();

    boost::function<bool(const char *)> cond1;
    cond1 = [](const char *val) {
        std::cout << string(val) << std::endl;
        return string(val) == "jako";
    };
    boost::function<bool(const char *)> cond2;
    cond2 = [](const char *val) {
        std::cout << *reinterpret_cast<const int *>(val) << std::endl;
        return *reinterpret_cast<const int *>(val) == 1;
    };
    auto result1 = tb.cond_on_data(1, cond1);
    std::cout << result1.size() << std::endl;
    auto result2 = tb.cond_on_data(0, cond2);
    std::cout << result2.size() << std::endl;
//    tb.drop();
}