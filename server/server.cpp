#include <iostream>
#include <sstream>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <fstream>
#include <ctime>
#include <vector>
#include <list>
#include <cstring>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
using namespace std;

const int MAX_KEYS = 5;



unordered_map<string, string> database;

unordered_map<string, time_t> expiry_times;


list<string> lru_list;
unordered_map<string, list<string>::iterator> lru_position;


mutex database_mutex;


string database_file = "database/dump.txt";
string aof_file =
    "database/appendonly.aof";
    const int AOF_REWRITE_THRESHOLD = 5;

int aof_command_count = 0;
    
    void append_to_aof(
    const string& command
)
{
    ofstream outfile(
        aof_file,
        ios::app
    );

    outfile
        << command
        << endl;

    outfile.close();
}

void replicate_command(const string& command) {

    int replica_socket =
        socket(AF_INET,
               SOCK_STREAM,
               0);

    if (replica_socket < 0)
        return;

    sockaddr_in replica_address;

    replica_address.sin_family =
        AF_INET;

    replica_address.sin_port =
        htons(8081);

      inet_pton(AF_INET,
          "127.0.0.1",
          &replica_address.sin_addr);

    if (connect(replica_socket,
                (sockaddr*)&replica_address,
                sizeof(replica_address)) < 0)
    {
        close(replica_socket);
        return;
    }

    send(replica_socket,
         command.c_str(),
         command.length() + 1,
         0);

    char buffer[1024];

    recv(replica_socket,
         buffer,
         sizeof(buffer),
         0);

    close(replica_socket);
}

void remove_from_lru_locked(const string& key) {
    auto it = lru_position.find(key);
    if (it != lru_position.end()) {
        lru_list.erase(it->second);
        lru_position.erase(it);
    }
}


void touch_lru_locked(const string& key) {
    remove_from_lru_locked(key);
    lru_list.push_front(key);
    lru_position[key] = lru_list.begin();
}


void rebuild_lru_from_database_locked() {
    lru_list.clear();
    lru_position.clear();

    for (const auto& entry : database) {
        lru_list.push_front(entry.first);
        lru_position[entry.first] = lru_list.begin();
    }
}


void save_database() {
    ofstream outfile(database_file);

    for (const auto& entry : database) {
        long long expiry = -1;

        auto it = expiry_times.find(entry.first);
        if (it != expiry_times.end()) {
            expiry = static_cast<long long>(it->second);
        }

        outfile << entry.first << " "
                << entry.second << " "
                << expiry << endl;
    }

    outfile.close();
}

void load_database() {
    ifstream infile(database_file);

    database.clear();
    expiry_times.clear();

    string line;
    while (getline(infile, line)) {
        if (line.empty()) continue;

        stringstream ss(line);
        string key, value;
        long long expiry = -1;

        if (!(ss >> key >> value)) {
            continue;
        }

        if (!(ss >> expiry)) {
            expiry = -1;
        }

        database[key] = value;

        if (expiry != -1) {
            expiry_times[key] = static_cast<time_t>(expiry);
        }
    }

    infile.close();
}

void replay_aof()
{
    ifstream infile(aof_file);

    string line;

    while (getline(infile, line))
    {
        if (line.empty())
            continue;

        stringstream ss(line);

        string operation;
        ss >> operation;

        if (operation == "SET")
        {
            string key;
            string value;

            ss >> key >> value;

            database[key] = value;
        }
        else if (operation == "DEL")
        {
            string key;

            ss >> key;

            database.erase(key);
        }
    }

    infile.close();
}
int count_aof_commands()
{
    ifstream infile(aof_file);

    string line;

    int count = 0;

    while (getline(infile, line))
    {
        if (!line.empty())
        {
            count++;
        }
    }

    infile.close();

    return count;
}

void rewrite_aof()
{
    ofstream outfile(
        "database/temp.aof"
    );

    for (const auto& entry : database)
    {
        outfile
            << "SET "
            << entry.first
            << " "
            << entry.second
            << endl;
    }

    outfile.close();

    remove(
        "database/appendonly.aof"
    );

    rename(
        "database/temp.aof",
        "database/appendonly.aof"
    );
    aof_command_count=count_aof_commands();

    cout
        << "AOF rewritten!"
        << endl;
}

bool cleanup_expired_keys_locked() {
    bool changed = false;
    time_t current_time = time(NULL);

    vector<string> expired_keys;

    for (const auto& entry : expiry_times) {
        const string& key = entry.first;
        time_t expiry_time = entry.second;

        if (current_time >= expiry_time) {
            expired_keys.push_back(key);
        }
    }

    for (const string& key : expired_keys) {
        database.erase(key);
        expiry_times.erase(key);
        remove_from_lru_locked(key);

        cout << "Expired key removed: " << key << endl;
        changed = true;
    }

    return changed;
}


bool evict_lru_key_locked() {
    if (lru_list.empty()) {
        return false;
    }

    string key = lru_list.back();
    lru_list.pop_back();
    lru_position.erase(key);
    database.erase(key);
    expiry_times.erase(key);

    cout << "Evicted LRU key: " << key << endl;
    return true;
}

// HANDLE ONE CLIENT
void handle_client(int client_socket) {
    cout << "Client connected!" << endl;

    char buffer[1024];

    while (true) {
        memset(buffer, 0, sizeof(buffer));

        int bytes_received =
            recv(client_socket,
                 buffer,
                 sizeof(buffer),
                 0);

        if (bytes_received <= 0)
            break;

        string command(buffer);

        cout << "Received Command: "
             << command << endl;

        stringstream ss(command);
        string operation;
        ss >> operation;

        // =====================
        // SET
        // =====================
        if (operation == "SET") {
            string key;
            string value;

            ss >> key >> value;

            int expiry_seconds = 0;
            bool has_expiry = false;

            if (ss >> expiry_seconds) {
                has_expiry = true;
            }

            string response = "OK";

            database_mutex.lock();

           
            cleanup_expired_keys_locked();

            bool key_exists = (database.count(key) > 0);

            // If this is a NEW key, make room using LRU
            if (!key_exists) {
                while (database.size() >= MAX_KEYS) {
                    if (!evict_lru_key_locked()) {
                        break;
                    }
                }
            }

            database[key] = value;

            if (has_expiry) {
                expiry_times[key] = time(NULL) + expiry_seconds;
            } else {
                expiry_times.erase(key);
            }

            touch_lru_locked(key);

            save_database();
           
              
append_to_aof(command);
aof_command_count++;


if (aof_command_count >=
    AOF_REWRITE_THRESHOLD)
{
    rewrite_aof();

   
}

            database_mutex.unlock();
              replicate_command(command);
            send(client_socket,
                 response.c_str(),
                 response.length() + 1,
                 0);
        }

        
        else if (operation == "GET") {
            string key;
            ss >> key;

            string response = "KEY NOT FOUND";
            bool changed = false;

            database_mutex.lock();

            
            if (database.size() > 10) {
                changed = cleanup_expired_keys_locked();
            }

            auto exp_it = expiry_times.find(key);

            if (exp_it != expiry_times.end()) {
                time_t expiry_time = exp_it->second;

                
                if (time(NULL) >= expiry_time || database.count(key) == 0) {
                    database.erase(key);
                    expiry_times.erase(key);
                    remove_from_lru_locked(key);
                    response = "KEY NOT FOUND";
                    changed = true;
                } else {
                    response = database[key];
                    touch_lru_locked(key);
                }
            } else {
                auto db_it = database.find(key);
                if (db_it != database.end()) {
                    response = db_it->second;
                    touch_lru_locked(key);
                }
            }

            if (changed) {
                save_database();
            }

            database_mutex.unlock();

            send(client_socket,
                 response.c_str(),
                 response.length() + 1,
                 0);
        }

       
        else if (operation == "DEL") {
            string key;
            ss >> key;

            database_mutex.lock();

            database.erase(key);
            expiry_times.erase(key);
            remove_from_lru_locked(key);

            save_database();
           
append_to_aof(command);
aof_command_count++;

if (aof_command_count >=
    AOF_REWRITE_THRESHOLD)
{
    rewrite_aof();

   
}

            database_mutex.unlock();
             replicate_command(command);
            string response = "DELETED";

            send(client_socket,
                 response.c_str(),
                 response.length() + 1,
                 0);
        }
        
        else if (operation == "SYNC")
{
    database_mutex.lock();

    string response;

    for (const auto& entry : database)
    {
        response +=
            "SET " +
            entry.first +
            " " +
            entry.second +
            "\n";
    }

    database_mutex.unlock();

    send(
        client_socket,
        response.c_str(),
        response.length() + 1,
        0
    );
}

        else {
            string response = "INVALID COMMAND";

            send(client_socket,
                 response.c_str(),
                 response.length() + 1,
                 0);
        }
    }

    cout << "Client disconnected!" << endl;
    close(client_socket);
}

int main() {
    cout << "MiniRedis Server Starting..." << endl;

    
    load_database();
    replay_aof();
    aof_command_count =
    count_aof_commands();

    database_mutex.lock();

    
    bool changed = cleanup_expired_keys_locked();

    
    rebuild_lru_from_database_locked();

    
    while (database.size() > MAX_KEYS) {
        if (!evict_lru_key_locked()) {
            break;
        }
        changed = true;
    }

    if (changed) {
        save_database();
    }

    database_mutex.unlock();

    cout << "Database loaded from disk!" << endl;

    int server_socket = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(8080);
    server_address.sin_addr.s_addr = INADDR_ANY;

    bind(server_socket,
         (struct sockaddr*)&server_address,
         sizeof(server_address));

    listen(server_socket, 5);

    cout << "MiniRedis Server listening on port 8080..." << endl;

    while (true) {
        int client_socket =
            accept(server_socket,
                   NULL,
                   NULL);

        thread t(handle_client, client_socket);
        t.detach();
    }

    close(server_socket);
    return 0;
}
