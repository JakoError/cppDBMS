//
// Created by JakoError on 2022/10/20.
//
#include "client.hpp"

#include <boost/regex.hpp>
#include <boost/exception/diagnostic_information.hpp>

#include <iostream>
#include <string>
#include <list>
#include <boost/algorithm/string/trim.hpp>

using std::cout;
using std::cin;
using std::cerr;
using std::endl;
using std::string;

namespace cppDBMS {

    void DBMSClient::connect() {
        for (int i = 0; i < connect_try_times; ++i) {
            try {
//                ep = ip::tcp::endpoint(ip::address::from_string(ip_address), port);
                sock = socket_ptr(new ip::tcp::socket(ios));
                sock->connect(ep);
            } catch (std::exception const &x) {
                std::cerr << boost::diagnostic_information(x) << std::endl;
                std::flush(std::cerr);
                system("pause");
                continue;
            }
            cout << "connected to server:" << ip_address << " port:" << port << endl;
            break;
        }
    }

    void DBMSClient::start() {
        char result[MAX_CMD_SIZE] = {};
        try {
            while (true) {
                if (!sock->is_open())
                    connect();

                cout << cmd_line;
                string cmd;
                getline(cin, cmd);
                boost::trim(cmd);

                if (cmd.length() == 0)
                    continue;
                if (cmd == "exit")
                    break;

                write(*sock, buffer(cmd));

                memset(result, 0, MAX_CMD_SIZE);
                size_t len = sock->read_some(buffer(result));
                if (len > 0)
                    cout << string(result) << endl;
                else
                    cout << "io read failed" << endl;
            }
        } catch (std::exception const &x) {
            std::cerr << boost::diagnostic_information(x) << std::endl;
            std::flush(std::cerr);
            system("pause");
        }
        sock->close();
    }
}

int main() {
    cppDBMS::DBMSClient client("127.0.0.1", 8000);
    client.connect();
    client.start();
}