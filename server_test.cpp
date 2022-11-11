//
// Created by JakoError on 2022/11/10.
//
#include "server.hpp"

boost::asio::ip::port_type port = 8000;

string data_path = "./data/";

int main() {
    io_service ios;
    DBMSServer Server(path(data_path), ios, ip::tcp::v4(), port);
    Server.start();
}