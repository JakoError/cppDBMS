//
// Created by JakoError on 2022/10/22.
//
#include "server.hpp"

namespace cppDBMS {
    void DBMSServer::client_exit(const DBMSServer::socket_ptr &sock) {
        client_num_mutex.lock();
        client_num--;
        cout << "client leave--client_ip:" << sock->remote_endpoint().address()
             << ", client_port:" << sock->remote_endpoint().port() << endl;
        client_num_mutex.unlock();
        sock->close();
    }

    void DBMSServer::client_session(const DBMSServer::socket_ptr &sock) {
        DBMS dbms(dbms_path);
        dbms.create();
        char data[MAX_CMD_SIZE] = {};
        try {
            while (true) {
                memset(data, 0, MAX_CMD_SIZE);
                size_t len = sock->read_some(buffer(data));
                if (len > 0) {
                    sql_mutex.lock();
                    write(*sock, buffer(dbms.process_sql(string(data))));
                    sql_mutex.unlock();
                } else {
                    std::cout << "io read failed" << endl;
                }
            }
        } catch (boost::system::system_error &x) {
            client_exit(sock);
            return;
        } catch (boost::exception &x) {
            std::cerr << boost::diagnostic_information(x) << std::endl;
            client_exit(sock);
            return;
        }
    }

    void DBMSServer::start() {
        try {
            while (true) {
                socket_ptr sock(new ip::tcp::socket(ios));
                acc.accept(*sock);
                cout << "accept new client:" << "clinet1" << " ip:" << sock->remote_endpoint().address() << endl;
                boost::thread([sock, this] { return client_session(sock); });
            }
        } catch (boost::exception &x) {
            std::cerr << boost::diagnostic_information(x) << std::endl;
        }
    }

}

int main() {
    cppDBMS::DBMSServer server(R"(D:\Codes\C++\cppDBMS\data\)", ip::tcp::v4(), 8000);
    server.start();
}