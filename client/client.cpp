#include <iostream>
#include <string>
#include <cstring>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <unistd.h>

using namespace std;

int main() {
    cout << "MiniRedis Client Starting..." << endl;

    int client_socket =
        socket(AF_INET,
               SOCK_STREAM,
               0);

    sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(8080);

    inet_pton(AF_INET,
              "127.0.0.1",
              &server_address.sin_addr);

    connect(client_socket,
            (struct sockaddr*)&server_address,
            sizeof(server_address));

    cout << "Connected to Redis Server!" << endl;

    while (true) {
        cout << "\nMiniRedis> ";

        string command;
        getline(cin, command);

        if (command == "EXIT")
            break;

        send(client_socket,
             command.c_str(),
             command.length() + 1,
             0);

        char buffer[1024];
        memset(buffer, 0, sizeof(buffer));

        recv(client_socket,
             buffer,
             sizeof(buffer),
             0);

        cout << buffer << endl;
    }

    close(client_socket);
    return 0;
}
