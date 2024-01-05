#include <filesystem>
#include <functional>
#include <iostream>
#include <dlfcn.h>
#include <vector>
#include <string>
#include <zmq.hpp>
#include <thread>
#include "worker.hpp"

namespace fs = std::filesystem;

struct KeyValue {
    std::string key;
    std::string value;
};

using MapFuncType = std::vector<KeyValue> (*)(KeyValue);
using ReduceFuncType = std::vector<std::string> (*)(std::vector<KeyValue>, int);

KeyValue get_content(const char* file);
void write_in_disk(const std::vector<KeyValue>& kvs, int map_task_idx);
void* reduce_worker();

void* map_worker() {
    zmq::context_t ctx(1);
    zmq::socket_t client(ctx, ZMQ_REQ);
    std::string server_address = "tcp://127.0.0.1:5555";
    client.connect(server_address);

    std::unique_lock<std::mutex> lock(map_mutex);
    int map_task_idx = map_id++;
    lock.unlock();
    bool ret = false;

    while(1) {
        ret = client.send("isMapDone", 10, ZMQ_SNDMORE);
        ret = client.send("",0);
        zmq::message_t reply;
        client.recv(&reply);
        if(std::string(static_cast<char*>(reply.data()), reply.size()) == "true") {
            cv.notify_all();
            return nullptr;
        }

        ret = client.send("assingTask", 10, ZMQ_SNDMORE);
        ret = client.send("",0);
        client.recv(&reply);
        std::string task_temp(static_cast<char*>(reply.data()), reply.size());

        if(task_temp == "empty") continue;
        std::cout << map_task_idx << " get the task: " << task_temp << " is stop " << std::endl;

        lock.lock();


        // ------------------------Test for timeout and retransmission---------------------
        // Note: Needs to match the map quantity specified by the master; in this case, 1, 3, 5 are disabled,
        // equivalent to threads 2, 4, 6 receiving tasks and then crashing
        // If only two map workers are allocated (0 working, 1 crashes), the timeout is set relatively long,
        // and one task is received after another; all tasks from 1 that timeout will be given to 0.
        if(disabled_map_id ==1 || disabled_map_id == 3 || disabled_map_id == 5) {
            disabled_map_id++;
            lock.unlock();
            std::cout << map_task_idx << " recv task: " << task_temp << " is stop\n";
        
            while(1) {
                std::this_thread::sleep_for(std::chrono::seconds(2));
            }
        } else {
            disabled_map_id++;
        }
        lock.unlock();
        char task[task_temp.size() + 1];
        strcpy(task, task_temp.c_str());
        KeyValue kv = get_content(task);

        MapFuncType map_func;
        std::vector<KeyValue> kvs = map_func(kv);
        write_in_disk(kvs, map_task_idx);

        std::cout << map_task_idx << " finish the task: " << task_temp << std::endl;

        ret = client.send("setMapStat", 10, ZMQ_SNDMORE);
        ret = client.send(task_temp.c_str(), task_temp.size());
        ret = client.send("", 0);
    }
}


int main() {
    fs::path mr_library_path = "./lib_mr_client.so";

    void* handle = dlopen(mr_library_path.c_str(), RTLD_LAZY);
    if (!handle) {
        std::cerr << "Cannot open library: " << dlerror() << '\n';
        exit(-1);
    }

    std::unique_ptr<void, decltype(&dlclose)> library_guard(handle, dlclose);

    MapFuncType map_func_ptr = reinterpret_cast<MapFuncType>(dlsym(handle, "map_func"));
    if (!map_func_ptr) {
        std::cerr << "Cannot load symbol 'map_func': " << dlerror() << '\n';
        exit(-1);
    }

    ReduceFuncType reduce_func_ptr = reinterpret_cast<ReduceFuncType>(dlsym(handle, "reduce_func"));
    if (!reduce_func_ptr) {
        std::cerr << "Cannot load symbol 'reduce_func': " << dlerror() << '\n';
        exit(-1);
    }

    zmq::context_t ctx(1);
    zmq::socket_t client(ctx, ZMQ_REQ);
    std::string server_address = "tcp://127.0.0.1:5555";
    client.setsockopt(ZMQ_RCVTIMEO, 5000);

    try {
        client.connect(server_address);

        //communication logic goes here
        std::thread tid_map[map_task_num];
        std::thread tid_reduce[reduce_task_num];

         // create map worker threads
        for (int i = 0; i < map_task_num; i++) {
            tid_map[i] = std::thread(map_worker);
        }

        // wait for all map workers to finish
        {
            std::unique_lock<std::mutex> lock(map_mutex);
            cv.wait(lock, [] { return done; });
        }

        for (int i = 0; i < reduce_task_num; i++) {
            tid_reduce[i] = std::thread(reduce_worker);
        }

        while (1) {
            if (done) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        for (int i = 0; i < map_task_num; i++) {
            tid_map[i].join();
        }

        for (int i = 0; i < reduce_task_num; i++) {
            tid_reduce[i].join();
        }

        client.close();

    } catch (const zmq::error_t& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    // MapFuncType map_func = map_func_ptr;
    // ReduceFuncType reduce_func = reduce_func_ptr;


    return 0;
}

