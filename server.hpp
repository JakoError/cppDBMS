//
// Created by JakoError on 2022/11/10.
//
#ifndef DBMS_SERVER_HPP
#define DBMS_SERVER_HPP

#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/regex.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/intrusive/bstree.hpp>
#include <boost/lexical_cast.hpp>

#include <filesystem>
#include <utility>
#include <fstream>
#include <string>
#include <utility>
#include <vector>
#include <iostream>

using namespace boost::asio;
using boost::regex;
using boost::trim;

using std::filesystem::path;
using std::cout;
using std::endl;
using std::string;
using std::vector;

#define MAX_CMD_SIZE 1024

#define STR_LENGTH 256

class DBMSServer {
private:
    typedef boost::shared_ptr<boost::asio::ip::tcp::socket> socket_ptr;

    int client_num = 0;

    path dbms_path;

    boost::mutex client_num_mutex;

    boost::mutex sql_mutex;

    //service instance
    io_service &ios;

    ip::tcp::endpoint ep;

    ip::tcp::acceptor acc;

    void client_exit(const socket_ptr &sock);

    void client_session(const socket_ptr &sock);

public:
    DBMSServer(path dbms_path, io_service &ios, ip::tcp protocal, ip::port_type port) :
            dbms_path(std::move(dbms_path)), ios(ios),
            ep(protocal, port),
            acc(ios, ep) {
    }

    [[noreturn]] void start();
};

#endif //DBMS_SERVER_HPP
