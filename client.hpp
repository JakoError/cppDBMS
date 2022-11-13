//
// Created by JakoError on 2022/11/13.
//

#ifndef DBMS_CLIENT_HPP
#define DBMS_CLIENT_HPP

#include <boost/asio.hpp>

#include <string>
#include <utility>

#ifndef MAX_CMD_SIZE
#define MAX_CMD_SIZE 1024
#endif //MAX_CMD_SIZE

using namespace boost::asio;

using std::string;

namespace cppDBMS {
    class DBMSClient {
    private:
        const string cmd_line = ">";

        const int connect_try_times = 5;
    public:
        typedef boost::shared_ptr<boost::asio::ip::tcp::socket> socket_ptr;

        string ip_address;
        boost::asio::ip::port_type port;

        io_service ios;

        ip::tcp::endpoint ep;

        socket_ptr sock;

        void connect();

        void start();

        DBMSClient(string ipAddress, boost::asio::ip::port_type port) :
                ip_address(std::move(ipAddress)), port(port),ep(ip::tcp::endpoint(ip::address::from_string(ip_address), port)),
                sock(new ip::tcp::socket(ios)){}
    };
}

#endif //DBMS_CLIENT_HPP
