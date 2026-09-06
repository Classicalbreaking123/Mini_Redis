#include <iostream>
#include <sstream>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <shared_mutex>
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
int current_size = 0;


unordered_map<string, string> database;

unordered_map<string, time_t> expiry_times;

list<string> lru_list;
unordered_map<string, list<string>::iterator> lru_position;


unordered_map<string, shared_mutex> key_mutex;



mutex lru_mutex;
mutex capacity_mutex;
mutex eviction_mutex;
string database_file = "database/dump.txt";
string aof_file =
    "database/appendonly.aof";
    const int AOF_REWRITE_THRESHOLD = 5;

int aof_command_count = 0;
    
    void remove_from_lru(const string& key);

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
bool remove_if_expired_locked(const string& key)
{
    auto it = expiry_times.find(key);

    if (it == expiry_times.end())
        return false;

    if (time(NULL) < it->second)
        return false;

   database.erase(key);
expiry_times.erase(it);

{
    lock_guard<mutex> lru_lock(lru_mutex);
    remove_from_lru(key);
}
    cout << "Expired key removed: "
         << key << endl;

    return true;
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
        htons(follower_port);

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


void heartbeat_worker()
{
    while (true)
    {
        int replica_socket =
            socket(AF_INET,
                   SOCK_STREAM,
                   0);

        if (replica_socket >= 0)
        {
            sockaddr_in replica_address;

            replica_address.sin_family = AF_INET;
            replica_address.sin_port = htons(8081);

            inet_pton(AF_INET,
                      "127.0.0.1",
                      &replica_address.sin_addr);
if (connect(replica_socket,
            (sockaddr*)&replica_address,
            sizeof(replica_address)) >= 0)
{
    cout << "Heartbeat sent!" << endl;

    string heartbeat = "HEARTBEAT";
    send(replica_socket,
         heartbeat.c_str(),
         heartbeat.length() + 1,
         0);

    char buffer[1024];
    recv(replica_socket,
         buffer,
         sizeof(buffer),
         0);
}
else
{
    perror("Heartbeat connect");
}

            close(replica_socket);
        }

        sleep(1);
    }
}

void remove_from_lru(const string& key) {
    auto it = lru_position.find(key);
    if (it != lru_position.end()) {
        lru_list.erase(it->second);
        lru_position.erase(it);
    }
}


void touch_lru(const string& key) {
    remove_from_lru(key);
    lru_list.push_front(key);
    lru_position[key] = lru_list.begin();
}


void rebuild_lru_from_database() {
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
        remove_from_lru(key);

        cout << "Expired key removed: " << key << endl;
        changed = true;
    }

    return changed;
}
void expiry_worker()
{
    while (true)
    {
        {
         

          bool changed = false;

for (auto it = database.begin(); it != database.end(); )
{
    unique_lock<shared_mutex> key_lock(key_mutex[it->first]);

    if (remove_if_expired_locked(it->first))
    {
        changed = true;
        it = database.begin();   // database changed
    }
    else
    {
        ++it;
    }
}

            if (changed)
            {
                save_database();
            }
        }

        this_thread::sleep_for(
            chrono::seconds(1));
    }
}


bool evict_lru_key()
{
    while (true)
    {
        string victim;

        // Step 1: Read current LRU tail
        {
            lock_guard<mutex> lru_lock(lru_mutex);

            if (lru_list.empty())
                return false;

            victim = lru_list.back();
        }

        // Step 2: Lock the victim key
        unique_lock<shared_mutex> victim_lock(key_mutex[victim]);

        // Step 3: Reacquire LRU and verify
        {
            lock_guard<mutex> lru_lock(lru_mutex);

            if (lru_list.empty())
                continue;

            if (lru_list.back() != victim)
                continue;

            // Victim is still the LRU
            lru_list.pop_back();
            lru_position.erase(victim);

            database.erase(victim);
            expiry_times.erase(victim);

            cout << "Evicted LRU key: "
                 << victim
                 << endl;

            return true;
        }
    }
}


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
unique_lock<shared_mutex> lock(key_mutex[key]);


           
           

            bool key_exists = (database.count(key) > 0);

            // If this is a NEW key, make room using LRU
          if (!key_exists)
{
    lock_guard<mutex> capacity_lock(capacity_mutex);

    if (current_size >= MAX_KEYS)
    {
        lock_guard<mutex> eviction_lock(eviction_mutex);

        while (current_size >= MAX_KEYS)
        {
            if (!evict_lru_key())
                break;

            current_size--;
        }
    }

    current_size++;
}

            database[key] = value;

            if (has_expiry) {
                expiry_times[key] = time(NULL) + expiry_seconds;
            } else {
                expiry_times.erase(key);
            }

           {
    lock_guard<mutex> lru_lock(lru_mutex);
    touch_lru(key);
}
            save_database();
           
              
append_to_aof(command);
aof_command_count++;


if (aof_command_count >=
    AOF_REWRITE_THRESHOLD)
{
    rewrite_aof();

   
}

        
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
            bool expired = false;

            // Step 1: Lock the key and read its value/expiry.
            // IMPORTANT: do not acquire lru_mutex while this key lock
            // is held. This avoids a key-lock -> LRU-lock cycle.
            {
                shared_lock<shared_mutex> key_lock(key_mutex[key]);

                auto expiry_it = expiry_times.find(key);

                if (expiry_it != expiry_times.end() &&
                    time(NULL) >= expiry_it->second)
                {
                    expired = true;
                }
                else
                {
                    auto it = database.find(key);

                    if (it != database.end())
                    {
                        response = it->second;
                    }
                }
            } // key_lock released

            // Step 2: Update LRU only after releasing the key lock.
            if (!expired && response != "KEY NOT FOUND")
            {
                lock_guard<mutex> lru_lock(lru_mutex);
                touch_lru(key);
            }

            // Step 3: If the key was expired, reacquire its key lock
            // and remove it. remove_if_expired_locked() may then acquire
            // lru_mutex, but we no longer hold the key lock elsewhere
            // while waiting for lru_mutex from the GET path.
            if (expired)
            {
                unique_lock<shared_mutex> key_lock(key_mutex[key]);

                changed = remove_if_expired_locked(key);

                if (changed)
                    save_database();

                response = "KEY NOT FOUND";
            }

            send(client_socket,
                 response.c_str(),
                 response.length() + 1,
                 0);
        }

        else if (operation == "DEL") {
            string key;
            ss >> key;

            unique_lock<shared_mutex> lock(key_mutex[key]);


            database.erase(key);
            expiry_times.erase(key);
    {
    lock_guard<mutex> lru_lock(lru_mutex);
    remove_from_lru(key);
}

            save_database();
           
append_to_aof(command);
aof_command_count++;

if (aof_command_count >=
    AOF_REWRITE_THRESHOLD)
{
    rewrite_aof();

   
}

          
             replicate_command(command);
            string response = "DELETED";

            send(client_socket,
                 response.c_str(),
                 response.length() + 1,
                 0);
        }
        
        else if (operation == "SYNC")
{



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
    current_size = database.size();
    aof_command_count =
    count_aof_commands();

   
bool changed = false;

{
    

    changed = cleanup_expired_keys_locked();
    current_size = database.size();
    rebuild_lru_from_database();

   while (current_size > MAX_KEYS)
{
    if (!evict_lru_key())
    {
        break;
    }

    current_size--;
    changed = true;
}

    if (changed)
    {
        save_database();
    }
}  

thread expiry(expiry_worker);
expiry.detach();
follower_port = REPLICA_PORT;
thread heartbeat(heartbeat_worker);
heartbeat.detach();
   

    cout << "Database loaded from disk!" << endl;

    int server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0)
    {
        perror("socket");
        return 1;
    }

    int opt = 1;
    setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in server_address{};
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(8080);
    server_address.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_socket,
         (struct sockaddr*)&server_address,
         sizeof(server_address)) < 0)
    {
        perror("bind");
        return 1;
    }

    if (listen(server_socket, 5) < 0)
    {
        perror("listen");
        return 1;
    }

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
