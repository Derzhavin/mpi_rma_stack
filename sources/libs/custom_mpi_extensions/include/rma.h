//
// Created by denis on 12.02.23.
//

#ifndef SOURCES_RMA_H
#define SOURCES_RMA_H

#include <mpi.h>
#include <utility>
#include <type_traits>

#include "MpiException.h"

namespace custom_mpi_extensions
{
    template<typename T, typename ...Args>
    MPI_Aint createObjectInRmaMemory(MPI_Win win, void* address, Args&&... arg)
    {
        address = nullptr;
        const auto size = sizeof(T);

        {
            auto mpiStatus = MPI_Alloc_mem(size, MPI_INFO_NULL, &address);
            if (mpiStatus != MPI_SUCCESS)
                throw MpiException("failed to allocate RMA memory", __FILE__, __func__, __LINE__, mpiStatus);
        }
        {
            auto mpiStatus = MPI_Win_attach(win, address, size);
            if (mpiStatus != MPI_SUCCESS)
                throw MpiException("failed to attach RMA window", __FILE__, __func__, __LINE__, mpiStatus);
        }

        try
        {
            new (address)T(std::forward<Args>(arg)...);
        }
        catch (std::exception& ex)
        {
            MPI_Free_mem(address);
            throw;
        }

        MPI_Aint disp;
        MPI_Get_address(address, &disp);
        return disp;
    }

    template<typename T>
    void freeRmaMemory(T* address) noexcept
    {
        if constexpr(!std::is_trivially_destructible_v<T>)
        {
            address->~T();
        }
        MPI_Free_mem(static_cast<void*>(address));
    }

}
#endif //SOURCES_RMA_H
