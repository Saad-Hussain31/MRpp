
#include <iostream>
#include <boost/mpi.hpp>

namespace mpi = boost::mpi;

int main(int argc, char* argv[]) {
    mpi::environment env(argc, argv);
    mpi::communicator world;

    int rank = world.rank();
    int size = world.size();

    std::cout << "Hello from rank " << rank << " of " << size << " processes." << std::endl;

    return 0;
}



