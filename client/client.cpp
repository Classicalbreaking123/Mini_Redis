#include <iostream>
#include <string>
#include <cstring>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <unistd.h>

using namespace std;

int connect_to_server(int port)
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in server_address{};
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(port);

    inet_pton(AF_INET, "127.0.0.1", &server_address.sin_addr);

    if (connect(sock, (sockaddr*)&server_address,
                sizeof(server_address)) < 0)
    {
        close(sock);
        return -1;
    }

    return sock;
}

bool replica_is_leader()
{
    int sock = connect_to_server(8081);

    if (sock < 0)
        return false;

    string command = "WHO_IS_LEADER";

    send(sock,
         command.c_str(),
         command.length() + 1,
         0);

    char buffer[1024];
    memset(buffer, 0, sizeof(buffer));

    int received =
        recv(sock,
             buffer,
             sizeof(buffer),
             0);

    close(sock);

    if (received <= 0)
        return false;

    // Replica replies "PRIMARY" after promotion.
    return string(buffer) == "PRIMARY";
}

int main()
{
    cout << "MiniRedis Client Starting..." << endl;

    int current_port = 8080;

    int client_socket =
        connect_to_server(current_port);

    if (client_socket < 0)
    {
        cout << "Could not connect!" << endl;
        return 0;
    }

    cout << "Connected to Redis Server!" << endl;

    while (true)
    {
        cout << "\nMiniRedis> ";

        string command;
        getline(cin, command);

        if (command == "EXIT")
            break;

        int sent =
            send(client_socket,
                 command.c_str(),
                 command.length() + 1,
                 0);

        cout << "send returned: " << sent << endl;

        char buffer[1024];
        memset(buffer, 0, sizeof(buffer));

        int received =
            recv(client_socket,
                 buffer,
                 sizeof(buffer),
                 0);

        cout << "recv returned: " << received << endl;

        if (received > 0 &&
            string(buffer) == "PRIMARY_RESTORED")
        {
            cout << "\nPrimary restored!" << endl;

            close(client_socket);

            client_socket =
                connect_to_server(8080);

            current_port = 8080;

            if (client_socket < 0)
            {
                cout << "Could not reconnect to primary." << endl;
                break;
            }

            cout << "Reconnected to primary." << endl;
            continue;
        }

        if (received <= 0)
        {
            cout << "\nConnection lost!" << endl;

            close(client_socket);

            if (replica_is_leader())
            {
                cout << "Replica is leader. Reconnecting..." << endl;

                client_socket =
                    connect_to_server(8081);

                current_port = 8081;

                if (client_socket < 0)
                {
                    cout << "Reconnect failed!" << endl;
                    break;
                }

                cout << "Connected to replica leader." << endl;

                cout << "Please enter the command again." << endl;
                continue;
            }

            cout << "No leader available." << endl;
            break;
        }

        cout << buffer << endl;
    }

    close(client_socket);
    return 0;
}

