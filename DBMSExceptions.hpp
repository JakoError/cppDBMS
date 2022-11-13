//
// Created by JakoError on 2022/11/12.
//

#ifndef DBMS_DBMSEXCEPTIONS_HPP
#define DBMS_DBMSEXCEPTIONS_HPP

#include <exception>

namespace cppDBMS {

    class cppDBMSException : public std::logic_error {
    public:
        using Base = std::logic_error;

        explicit cppDBMSException(const char *const message) : Base(message) {}

        explicit cppDBMSException(const std::string &message) : Base(message.c_str()) {}

        [[nodiscard]] const char *what() const noexcept override {
            return logic_error::what();
        }


        virtual std::string getName() {
            return "cppDBMSException";
        }
    };

    class SystemException : public cppDBMSException {
    public:
        using Base = cppDBMSException;

        explicit SystemException(const char *const message) : Base(message) {}

        explicit SystemException(const std::string &message) : Base(message.c_str()) {}

        [[nodiscard]] const char *what() const noexcept override {
            return Base::what();
        }

        std::string getName() override{
            return "SystemException";
        }
    };

    class SqlException : public cppDBMSException {
    public:
        using Base = cppDBMSException;

        explicit SqlException(const char *const message) : Base(message) {}

        explicit SqlException(const std::string &message) : Base(message.c_str()) {}

        [[nodiscard]] const char *what() const noexcept override {
            return Base::what();
        }

        std::string getName() override{
            return "SqlException";
        }
    };

    class ParserException : public cppDBMSException {
    public:
        using Base = cppDBMSException;

        explicit ParserException(const char *const message) : Base(message) {}

        explicit ParserException(const std::string &message) : Base(message.c_str()) {}

        [[nodiscard]] const char *what() const noexcept override {
            return Base::what();
        }

        std::string getName() override{
            return "ParserException";
        }
    };

    class IOException : public cppDBMSException {
    public:
        using Base = cppDBMSException;

        explicit IOException(const char *const message) : Base(message) {}

        explicit IOException(const std::string &message) : Base(message.c_str()) {}

        [[nodiscard]] const char *what() const noexcept override {
            return Base::what();
        }

        std::string getName() override{
            return "IOException";
        }
    };
} // cppDBMS

#endif //DBMS_DBMSEXCEPTIONS_HPP
