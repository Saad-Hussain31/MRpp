#include <mutex>
#include <condition_variable>

int map_id = 0;
int disabled_map_id = 0;
int disabled_reduce_id = 0;
std::mutex map_mutex;
std::condition_variable cv;
int file_id = 0;
bool done{false};

// Define the number of map and reduce tasks assigned by the master
int map_task_num;
int reduce_task_num;
