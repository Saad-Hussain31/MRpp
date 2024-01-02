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

void* map_worker() {
    {
        std::lock_guard<std::mutex> lock(map_mutex);
        done = true;
    }
    cv.notify_one();
};
void* reduce_worker();

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
        std::cerr << "Cannot load symbol 'mapF': " << dlerror() << '\n';
        exit(-1);
    }

    ReduceFuncType reduce_func_ptr = reinterpret_cast<ReduceFuncType>(dlsym(handle, "reduce_func"));
    if (!reduce_func_ptr) {
        std::cerr << "Cannot load symbol 'reduceF': " << dlerror() << '\n';
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
