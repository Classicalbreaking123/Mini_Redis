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
shared_mutex database_mutex;
mutex aof_rewrite_mutex;
mutex database_save_mutex;


mutex lru_mutex;
mutex capacity_mutex;
mutex eviction_mutex;
string database_file = "database/dump.txt";
string aof_file =
    "database/appendonly.aof";
    const int AOF_REWRITE_THRESHOLD = 27;

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
remove_from_lru(key);
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


void save_database_locked()
{
    const string temp_database_file = "database/temp_dump.txt";

    ofstream outfile(temp_database_file);

    if (!outfile.is_open())
    {
        cerr << "Failed to create temporary database file!" << endl;
        return;
    }

    for (const auto& entry : database)
    {
        long long expiry = -1;

        auto it = expiry_times.find(entry.first);

        if (it != expiry_times.end())
        {
            expiry = static_cast<long long>(it->second);
        }

        outfile
            << entry.first << " "
            << entry.second << " "
            << expiry
            << endl;
    }

    outfile.close();

    if (!outfile)
    {
        cerr << "Failed while writing temporary database file!" << endl;
        remove(temp_database_file.c_str());
        return;
    }

    // Replace dump.txt only after the new snapshot is complete.
    if (rename(temp_database_file.c_str(), database_file.c_str()) != 0)
    {
        cerr << "Failed to replace database dump file!" << endl;
        remove(temp_database_file.c_str());
        return;
    }

    // Create a fresh empty AOF and atomically replace the old one.
    const string temp_aof_file = "database/temp_appendonly.aof";

    ofstream new_aof(temp_aof_file);

    if (!new_aof.is_open())
    {
        cerr << "Failed to create temporary AOF file!" << endl;
        return;
    }

    new_aof.close();

    if (!new_aof)
    {
        cerr << "Failed while creating temporary AOF file!" << endl;
        remove(temp_aof_file.c_str());
        return;
    }

    if (rename(temp_aof_file.c_str(), aof_file.c_str()) != 0)
    {
        cerr << "Failed to replace AOF file!" << endl;
        remove(temp_aof_file.c_str());
        return;
    }

    aof_command_count = 0;

    cout << "Database saved!" << endl;
}

void save_database()
{
    // Only one database save can run at a time.
    lock_guard<mutex> save_lock(database_save_mutex);

    // A previous save may have already completed while this thread
    // was waiting for the save mutex. A successful save resets the AOF
    // command count, so there is nothing left to save for this trigger.
    if (aof_command_count == 0)
        return;

    // GET operations can continue because this is a shared lock.
    // SET, DEL, expiry cleanup and eviction require the unique lock
    // and therefore wait until the complete save finishes.
    shared_lock<shared_mutex> database_lock(database_mutex);

    save_database_locked();
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
    // Only one AOF rewrite can run at a time.
    lock_guard<mutex> rewrite_lock(aof_rewrite_mutex);

    // Re-check after acquiring the rewrite lock.
    // Another thread may have already completed the rewrite.
    if (aof_command_count < AOF_REWRITE_THRESHOLD)
        return;

    // Shared database lock allows GET operations while blocking SET/DEL and
    // other operations that modify the database.
    shared_lock<shared_mutex> database_lock(database_mutex);

    const string temp_aof_file = "database/temp.aof";

    ofstream outfile(temp_aof_file);

    if (!outfile.is_open())
    {
        cerr << "Failed to create temporary AOF file!" << endl;
        return;
    }

    // Write the new AOF from the current live database.
    // SET/DEL cannot change the database while this lock is held.
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

    if (!outfile)
    {
        cerr << "Failed while writing temporary AOF file!" << endl;
        remove(temp_aof_file.c_str());
        return;
    }

    // rename() replaces the destination atomically on POSIX systems.
    if (rename(temp_aof_file.c_str(), aof_file.c_str()) != 0)
    {
        cerr << "Failed to replace AOF file!" << endl;
        remove(temp_aof_file.c_str());
        return;
    }

    aof_command_count = count_aof_commands();

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

            // Global lock order: database -> LRU -> key.
            unique_lock<shared_mutex> database_lock(database_mutex);
            lock_guard<mutex> lru_lock(lru_mutex);

            for (auto it = database.begin(); it != database.end(); )
            {
                string key = it->first;
                unique_lock<shared_mutex> key_lock(key_mutex[key]);

                if (remove_if_expired_locked(key))
                {
                    changed = true;
                    it = database.begin();
                }
                else
                {
                    ++it;
                }
            }

            if (changed)
            {
                save_database_locked();
            }
        }

        this_thread::sleep_for(
            chrono::seconds(1));
    }
}


bool evict_lru_key()
{
    unique_lock<shared_mutex> database_lock(database_mutex);
    lock_guard<mutex> lru_lock(lru_mutex);

    if (lru_list.empty())
        return false;

    string victim = lru_list.back();
    unique_lock<shared_mutex> victim_lock(key_mutex[victim]);

    // Re-check after acquiring the victim key lock.
    if (lru_position.find(victim) == lru_position.end())
        return false;

    if (lru_list.back() != victim)
        return false;

    lru_list.pop_back();
    lru_position.erase(victim);
    database.erase(victim);
    expiry_times.erase(victim);

    cout << "Evicted LRU key: "
         << victim
         << endl;

    return true;
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
            bool should_rewrite = false;

            {
                // Global lock order: database -> LRU -> capacity -> eviction -> key.
                unique_lock<shared_mutex> database_lock(database_mutex);
                lock_guard<mutex> lru_lock(lru_mutex);
                unique_lock<shared_mutex> key_lock(key_mutex[key]);

                // Re-check after acquiring locks.
                bool key_exists = (database.count(key) > 0);

                if (!key_exists)
                {
                    lock_guard<mutex> capacity_lock(capacity_mutex);

                    if (current_size >= MAX_KEYS)
                    {
                        lock_guard<mutex> eviction_lock(eviction_mutex);

                        while (current_size >= MAX_KEYS)
                        {
                            if (lru_list.empty())
                                break;

                            string victim = lru_list.back();
                            unique_lock<shared_mutex> victim_lock(key_mutex[victim]);

                            // Re-check after acquiring the victim key lock.
                            if (lru_position.find(victim) == lru_position.end())
                                continue;

                            if (lru_list.back() != victim)
                                continue;

                            lru_list.pop_back();
                            lru_position.erase(victim);
                            database.erase(victim);
                            expiry_times.erase(victim);
                            current_size--;

                            cout << "Evicted LRU key: "
                                 << victim << endl;
                        }
                    }

                    if (current_size < MAX_KEYS)
                        current_size++;
                }

                database[key] = value;

                if (has_expiry) {
                    expiry_times[key] = time(NULL) + expiry_seconds;
                } else {
                    expiry_times.erase(key);
                }

                touch_lru(key);

                // Keep the database lock until this mutation is persisted
                // in the AOF, so rewrite cannot pass between the two.
                save_database_locked();
                append_to_aof(command);

                aof_command_count++;

                if (aof_command_count >= AOF_REWRITE_THRESHOLD)
                    should_rewrite = true;
            }

            if (should_rewrite)
                rewrite_aof();

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
            bool expired = false;

            // Shared database lock allows normal GETs during AOF rewrite.
            {
                shared_lock<shared_mutex> database_lock(database_mutex);
                lock_guard<mutex> lru_lock(lru_mutex);
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
                        touch_lru(key);
                    }
                }
            }

            // Expired-key cleanup modifies the database, so it needs an
            // exclusive database lock. Re-check after acquiring it.
            if (expired)
            {
                unique_lock<shared_mutex> database_lock(database_mutex);
                lock_guard<mutex> lru_lock(lru_mutex);
                unique_lock<shared_mutex> key_lock(key_mutex[key]);

                auto expiry_it = expiry_times.find(key);

                if (expiry_it != expiry_times.end() &&
                    time(NULL) >= expiry_it->second)
                {
                    database.erase(key);
                    expiry_times.erase(expiry_it);
                    remove_from_lru(key);

                    if (current_size > 0)
                        current_size--;

                    save_database_locked();
                }
                else
                {
                    auto it = database.find(key);

                    if (it != database.end())
                    {
                        response = it->second;
                        touch_lru(key);
                    }
                }
            }

            send(client_socket,
                 response.c_str(),
                 response.length() + 1,
                 0);
        }

        else if (operation == "DEL") {
            string key;
            ss >> key;

            bool should_rewrite = false;

            {
                // Global lock order: database -> LRU -> key.
                unique_lock<shared_mutex> database_lock(database_mutex);
                lock_guard<mutex> lru_lock(lru_mutex);
                unique_lock<shared_mutex> key_lock(key_mutex[key]);

                // Re-check after acquiring locks.
                bool key_exists = (database.count(key) > 0);

                database.erase(key);
                expiry_times.erase(key);
                remove_from_lru(key);

                if (key_exists && current_size > 0)
                    current_size--;

                // Keep the database lock until DELETE is in the AOF.
                save_database_locked();
                append_to_aof(command);

                aof_command_count++;

                if (aof_command_count >= AOF_REWRITE_THRESHOLD)
                    should_rewrite = true;
            }

            if (should_rewrite)
                rewrite_aof();

            replicate_command(command);
            string response = "DELETED";

            send(client_socket,
                 response.c_str(),
                 response.length() + 1,
                 0);
        }

        else if (operation == "SYNC")
{
    shared_lock<shared_mutex> database_lock(database_mutex);

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
