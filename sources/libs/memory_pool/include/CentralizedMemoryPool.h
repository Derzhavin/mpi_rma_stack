//
// Created by denis on 18.03.23.
//

#ifndef RMA_LIST_SKETCH_MEMORYPOOL_H
#define RMA_LIST_SKETCH_MEMORYPOOL_H

#include <mpi.h>
#include <memory>
#include <cassert>
#include <spdlog/spdlog.h>

#include "MpiWinWrapper.h"
#include "MpiException.h"
#include "rma.h"
#include "memory_pool.h"
using namespace std::literals::string_literals;

namespace custom_mpi = custom_mpi_extensions;

template<typename T>
class CentralizedMemoryPool
{
    typedef void (*MemoryUsageDeleter_t)(MemoryUsageMark*);
    typedef void (*MemoryDeleter_t)(T*);

public:
    constexpr inline static int CENTRAL_RANK{0};

    CentralizedMemoryPool(MPI_Comm t_comm, MPI_Datatype t_datatype, MPI_Aint t_elemsNumber, std::shared_ptr<spdlog::logger> t_logger);
    ~CentralizedMemoryPool() = default;
    CentralizedMemoryPool(CentralizedMemoryPool&) = delete;
    CentralizedMemoryPool(CentralizedMemoryPool&& t_other) noexcept;
    CentralizedMemoryPool& operator=(CentralizedMemoryPool&) = delete;
    CentralizedMemoryPool& operator=(CentralizedMemoryPool&& t_other) noexcept;

    [[nodiscard]] bool isCentralRank() const;

    MPI_Aint allocate();
    void deallocate(MPI_Aint elemMemoryAddress);

private:
    void initMemoryPool();

private:
    std::shared_ptr<spdlog::logger> m_logger;

    std::unique_ptr<T[], MemoryDeleter_t> m_pMemory{nullptr, nullptr};
    std::unique_ptr<MemoryUsageMark[], MemoryUsageDeleter_t> m_pMemoryUsage{nullptr, nullptr};
    MPI_Comm m_comm{MPI_COMM_NULL};
    int m_rank{-1};
    MPI_Aint m_elemsNumber{0};
    MPI_Datatype m_datatype{MPI_DATATYPE_NULL};
    MPI_Aint m_elemsBaseAddress{(MPI_Aint) MPI_BOTTOM};
    MPI_Aint m_memoryUsageBaseAddress{(MPI_Aint) MPI_BOTTOM};
    custom_mpi::MpiWinWrapper m_elemsWinWrapper;
    custom_mpi::MpiWinWrapper m_memoryUsageWinWrapper;
};

template<typename T>
CentralizedMemoryPool<T>::CentralizedMemoryPool(MPI_Comm t_comm, MPI_Datatype t_datatype, MPI_Aint t_elemsNumber, std::shared_ptr<spdlog::logger> t_logger)
:
m_logger(std::move(t_logger)),
m_comm(t_comm),
m_datatype(t_datatype),
m_elemsNumber(t_elemsNumber),
m_elemsWinWrapper(MPI_INFO_NULL, t_comm),
m_memoryUsageWinWrapper(MPI_INFO_NULL, t_comm)
{
    {
        auto mpiStatus = MPI_Comm_rank(m_comm, &m_rank);
        if (mpiStatus != MPI_SUCCESS)
            throw custom_mpi_extensions::MpiException("failed to get rank", __FILE__, __func__, __LINE__, mpiStatus);
    }

    initMemoryPool();
}

template<typename T>
MPI_Aint CentralizedMemoryPool<T>::allocate()
{
    MPI_Aint elemMemoryAddress{(MPI_Aint)MPI_BOTTOM};
    auto win = m_memoryUsageWinWrapper.getMpiWin();

    for (MPI_Aint disp = 0; disp < m_elemsNumber; ++disp)
    {
        MemoryUsageMark oldState{Released};
        MemoryUsageMark newState{Acquired};
        MemoryUsageMark resState{Acquired};

        auto memoryUsageAddress = MPI_Aint_add(m_memoryUsageBaseAddress, disp);

        MPI_Win_lock_all(0, win);
        MPI_Compare_and_swap(&newState, &oldState, &resState, MPI_INT, CENTRAL_RANK, memoryUsageAddress, win);
        MPI_Win_flush_all(win);
        MPI_Win_unlock_all(win);

        SPDLOG_LOGGER_TRACE(m_logger, "acquired memory usage address '{}'", memoryUsageAddress);

        if (resState == MemoryUsageMark::Released)
        {
            elemMemoryAddress = MPI_Aint_add(m_elemsBaseAddress, disp);
            break;
        }
    }

    SPDLOG_LOGGER_TRACE(m_logger, "acquired element memory address '{}'", elemMemoryAddress);
    return elemMemoryAddress;
}

template<typename T>
void CentralizedMemoryPool<T>::deallocate(MPI_Aint elemMemoryAddress)
{
    auto win = m_memoryUsageWinWrapper.getMpiWin();

    MemoryUsageMark oldState{Acquired};
    MemoryUsageMark newState{Released};
    MemoryUsageMark resState{Released};

    auto disp = MPI_Aint_diff(m_elemsBaseAddress, elemMemoryAddress);
    auto memoryUsageAddress = MPI_Aint_add(m_memoryUsageBaseAddress, disp);

    MPI_Win_lock_all(0, win);
    MPI_Compare_and_swap(&newState, &oldState, &resState, MPI_INT, CENTRAL_RANK, memoryUsageAddress, win);
    MPI_Win_flush_all(win);
    MPI_Win_unlock_all(win);

    assert(resState == MemoryUsageMark::Acquired);
    SPDLOG_LOGGER_TRACE(m_logger, "released memory usage address - {}", memoryUsageAddress);
    SPDLOG_LOGGER_TRACE(m_logger, "released element memory address - {}", elemMemoryAddress);
}

template<typename T>
bool CentralizedMemoryPool<T>::isCentralRank() const
{
    return m_rank == CENTRAL_RANK;
}

template<typename T>
CentralizedMemoryPool<T>::CentralizedMemoryPool(CentralizedMemoryPool &&t_other) noexcept
:
m_pMemory(std::move(t_other.m_pMemory)),
m_pMemoryUsage(std::move(t_other.m_pMemoryUsage)),
m_comm(t_other.m_comm),
m_rank(std::exchange(t_other.m_rank, -1)),
m_elemsNumber(std::exchange(t_other.m_elemsNumber, 0)),
m_datatype(t_other.m_datatype),
m_elemsBaseAddress(std::exchange(t_other.m_elemsBaseAddress, (MPI_Aint)MPI_BOTTOM)),
m_elemsWinWrapper(std::exchange(t_other.m_elemsWinWrapper, custom_mpi_extensions::MpiWinWrapper()))
{

}

template<typename T>
CentralizedMemoryPool<T> &CentralizedMemoryPool<T>::operator=(CentralizedMemoryPool &&t_other) noexcept
{
    if (&t_other != this)
    {
        m_pMemory = std::exchange(t_other.m_pMemory, nullptr);
        m_comm = t_other.m_comm;
        m_rank = std::exchange(t_other.m_rank, -1);
        m_elemsNumber = std::exchange(t_other.m_elemsNumber, 0);
        m_datatype = t_other.m_datatype;
        m_elemsBaseAddress = std::exchange(t_other.m_elemsBaseAddress, (MPI_Aint)MPI_BOTTOM);
        m_elemsWinWrapper = std::exchange(t_other.m_elemsWinWrapper, custom_mpi_extensions::MpiWinWrapper());
    }
    return *this;
}

template<typename T>
void CentralizedMemoryPool<T>::initMemoryPool()
{
    if (isCentralRank())
    {
        MemoryUsageMark *pMemoryUsage{nullptr};
        auto memoryUsageWin = m_memoryUsageWinWrapper.getMpiWin();
        {
            auto mpiStatus = MPI_Alloc_mem(sizeof(MemoryUsageMark) * m_elemsNumber, MPI_INFO_NULL, &pMemoryUsage);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to allocate RMA memory", __FILE__, __func__, __LINE__,
                                               mpiStatus);
        }
        m_pMemoryUsage = std::unique_ptr<MemoryUsageMark[], MemoryUsageDeleter_t>(pMemoryUsage, [] (MemoryUsageMark* p) {
            MPI_Free_mem(p);
        });
        std::fill_n(m_pMemoryUsage.get(), m_elemsNumber, MemoryUsageMark::Released);
        {
            auto mpiStatus = MPI_Win_attach(memoryUsageWin, pMemoryUsage, m_elemsNumber);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to attach RMA window", __FILE__, __func__, __LINE__, mpiStatus);
        }
        MPI_Get_address(pMemoryUsage, &m_memoryUsageBaseAddress);

        auto memoryWin = m_elemsWinWrapper.getMpiWin();
        T* pElems{nullptr};
        int elemSize{0};
        MPI_Type_size(m_datatype, &elemSize);
        {
            auto mpiStatus = MPI_Alloc_mem(elemSize * m_elemsNumber, MPI_INFO_NULL, &pElems);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to allocate RMA memory", __FILE__, __func__, __LINE__, mpiStatus);
        }
        m_pMemory = std::unique_ptr<T[], MemoryDeleter_t>(pElems, [] (T* p) {
            if constexpr(!std::is_trivially_destructible_v<T>)
            {
                p->~T();
            }
            MPI_Free_mem(p);
        });
        {
            auto mpiStatus = MPI_Win_attach(memoryWin, pElems, elemSize * m_elemsNumber);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to attach RMA window", __FILE__, __func__, __LINE__, mpiStatus);
        }
        MPI_Get_address(pElems, &m_elemsBaseAddress);
    }
    MPI_Bcast(&m_elemsBaseAddress, 1, m_datatype, CENTRAL_RANK, m_comm);
    MPI_Bcast(&m_memoryUsageBaseAddress, 1, MPI_AINT, CENTRAL_RANK, m_comm);
}

#endif //RMA_LIST_SKETCH_MEMORYPOOL_H
