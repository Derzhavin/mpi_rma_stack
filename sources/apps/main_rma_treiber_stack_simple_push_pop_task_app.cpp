//
// Created by denis on 22.01.23.
//

#include <iostream>
#include <chrono>

#include "RmaTreiberCentralStack.h"
#include "include/stack_tasks.h"

using namespace std::literals;

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);
    MPI_Comm comm = MPI_COMM_WORLD;
    MPI_Comm_split_type(comm, MPI_COMM_TYPE_SHARED, 0 /* key */, MPI_INFO_NULL, &comm);
    auto returnCode{EXIT_SUCCESS};

    const auto minBackoffDelay = 1ns;
    const auto maxBackoffDelay = 100ns;
    const auto freeNodesLimit{10};
    try
    {
        rma_stack::RmaTreiberCentralStack<int> rmaTreiberStack(comm, MPI_INT, minBackoffDelay, maxBackoffDelay,
                                                               freeNodesLimit, <#initializer#>);
        runSimplePushPopTask(rmaTreiberStack);
    }
    catch (custom_mpi_extensions::MpiException& ex)
    {
        std::cerr << ex;
        returnCode = EXIT_FAILURE;
    }
    catch (std::exception& ex)
    {
        std::cerr << "Unexpected exception: " << ex.what();
        returnCode = EXIT_FAILURE;
    }

    MPI_Finalize();
    return returnCode;
}