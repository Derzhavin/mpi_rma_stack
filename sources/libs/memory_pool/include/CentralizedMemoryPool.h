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
    typedef std::function<void(MemoryUsageMark*)> MemoryUsageDeleter_t;
    typedef std::function<void(T*)> MemoryDeleter_t;

    static constexpr auto m_sMemoryUsageMarkSize = static_cast<MPI_Aint>(sizeof(MemoryUsageMark));

    constexpr inline static int CENTRAL_RANK{0};
public:

    CentralizedMemoryPool(MPI_Comm comm, MPI_Datatype t_datatype, MPI_Aint t_elemsNumber, std::shared_ptr<spdlog::logger> t_logger);
    ~CentralizedMemoryPool() = default;
    CentralizedMemoryPool(CentralizedMemoryPool&) = delete;
    CentralizedMemoryPool(CentralizedMemoryPool&& t_other) noexcept;
    CentralizedMemoryPool& operator=(CentralizedMemoryPool&) = delete;
    CentralizedMemoryPool& operator=(CentralizedMemoryPool&& t_other) noexcept;

    MPI_Aint allocate();
    void deallocate(MPI_Aint elemMemoryAddress);

    [[nodiscard]] const custom_mpi::MpiWinWrapper &getElemsWinWrapper() const;
    [[nodiscard]] MPI_Datatype getDatatype() const;
    [[nodiscard]] int getMpiDatatypeSize() const;

private:
    void initMemoryPool(MPI_Comm comm);
    [[nodiscard]] bool isCentralRank() const;

private:
    std::shared_ptr<spdlog::logger> m_logger;

    std::unique_ptr<T[], MemoryDeleter_t> m_pMemory{nullptr, nullptr};
    std::unique_ptr<MemoryUsageMark[], MemoryUsageDeleter_t> m_pMemoryUsage{nullptr, nullptr};
    int m_rank{-1};
    int m_mpiElemSize{0};
    MPI_Aint m_elemsNumber{0};
    MPI_Datatype m_datatype{MPI_DATATYPE_NULL};
    MPI_Aint m_elemsBaseAddress{(MPI_Aint) MPI_BOTTOM};
    MPI_Aint m_memoryUsageBaseAddress{(MPI_Aint) MPI_BOTTOM};
    custom_mpi::MpiWinWrapper m_elemsWinWrapper;
    custom_mpi::MpiWinWrapper m_memoryUsageWinWrapper;
};

template<typename T>
CentralizedMemoryPool<T>::CentralizedMemoryPool(MPI_Comm comm, MPI_Datatype t_datatype, MPI_Aint t_elemsNumber, std::shared_ptr<spdlog::logger> t_logger)
:
m_logger(std::move(t_logger)),
m_datatype(t_datatype),
m_elemsNumber(t_elemsNumber),
m_elemsWinWrapper(MPI_INFO_NULL, comm),
m_memoryUsageWinWrapper(MPI_INFO_NULL, comm)
{
    {
        auto mpiStatus = MPI_Comm_rank(comm, &m_rank);
        if (mpiStatus != MPI_SUCCESS)
            throw custom_mpi_extensions::MpiException("failed to get rank", __FILE__, __func__, __LINE__, mpiStatus);
    }

    initMemoryPool(0);
}

template<typename T>
MPI_Aint CentralizedMemoryPool<T>::allocate()
{
    auto elemMemoryAddress{(MPI_Aint)MPI_BOTTOM};
    auto memoryUsageAddress{(MPI_Aint)MPI_BOTTOM};
    auto win = m_memoryUsageWinWrapper.getMpiWin();

    for (MPI_Aint disp = 0; disp < m_elemsNumber; ++disp)
    {
        MemoryUsageMark oldState{Released};
        MemoryUsageMark newState{Acquired};
        MemoryUsageMark resState{Acquired};

        memoryUsageAddress = MPI_Aint_add(m_memoryUsageBaseAddress, disp * m_sMemoryUsageMarkSize);

        SPDLOG_LOGGER_TRACE(m_logger, "trying to acquire memory usage m_address {}", memoryUsageAddress);

        MPI_Win_lock_all(0, win);
        MPI_Compare_and_swap(&newState, &oldState, &resState, MPI_INT, CENTRAL_RANK, memoryUsageAddress, win);
        MPI_Win_flush_all(win);
        MPI_Win_unlock_all(win);

        if (resState == MemoryUsageMark::Released)
        {
            elemMemoryAddress = MPI_Aint_add(m_elemsBaseAddress, disp * m_mpiElemSize);
            break;
        }
        else if (resState != MemoryUsageMark::Acquired)
        {
            SPDLOG_LOGGER_TRACE(m_logger, "invalid memory usage m_address mark {} to memory usage m_address {}", resState, memoryUsageAddress);
            assert(resState == MemoryUsageMark::Acquired);
        }
    }

    SPDLOG_LOGGER_TRACE(m_logger, "acquired memory usage m_address {}", memoryUsageAddress);
    SPDLOG_LOGGER_TRACE(m_logger, "acquired element memory m_address {}", elemMemoryAddress);
    return elemMemoryAddress;
}

template<typename T>
void CentralizedMemoryPool<T>::deallocate(MPI_Aint elemMemoryAddress)
{
    auto win = m_memoryUsageWinWrapper.getMpiWin();

    MemoryUsageMark oldState{Acquired};
    MemoryUsageMark newState{Released};
    MemoryUsageMark resState{Released};

    auto disp = MPI_Aint_diff(elemMemoryAddress, m_elemsBaseAddress) / m_mpiElemSize;
    auto memoryUsageAddress = MPI_Aint_add(m_memoryUsageBaseAddress, disp * m_sMemoryUsageMarkSize);

    SPDLOG_LOGGER_TRACE(m_logger, "trying to release memory usage m_address {}", memoryUsageAddress);

    MPI_Win_lock_all(0, win);
    MPI_Compare_and_swap(&newState, &oldState, &resState, m_datatype, CENTRAL_RANK, memoryUsageAddress, win);
    MPI_Win_flush_all(win);
    MPI_Win_unlock_all(win);

    if (resState != MemoryUsageMark::Acquired)
    {
        SPDLOG_LOGGER_TRACE(m_logger, "invalid memory usage m_address mark {} to memory usage m_address {}", resState, memoryUsageAddress);
        assert(resState == MemoryUsageMark::Acquired);
    }

    SPDLOG_LOGGER_TRACE(m_logger, "released memory usage m_address {}", memoryUsageAddress);
    SPDLOG_LOGGER_TRACE(m_logger, "released element memory m_address {}", elemMemoryAddress);
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
        m_rank = std::exchange(t_other.m_rank, -1);
        m_elemsNumber = std::exchange(t_other.m_elemsNumber, 0);
        m_datatype = t_other.m_datatype;
        m_elemsBaseAddress = std::exchange(t_other.m_elemsBaseAddress, (MPI_Aint)MPI_BOTTOM);
        m_elemsWinWrapper = std::exchange(t_other.m_elemsWinWrapper, custom_mpi_extensions::MpiWinWrapper());
    }
    return *this;
}

template<typename T>
void CentralizedMemoryPool<T>::initMemoryPool(MPI_Comm comm)
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
        m_pMemoryUsage = std::unique_ptr<MemoryUsageMark[], MemoryUsageDeleter_t>(pMemoryUsage, [logger=m_logger] (MemoryUsageMark* p) {
            MPI_Free_mem(p);
            SPDLOG_LOGGER_TRACE(logger, "freed up memory usage marks' memory");
        });
        std::fill_n(m_pMemoryUsage.get(), m_elemsNumber, MemoryUsageMark::Released);
        {
            auto mpiStatus = MPI_Win_attach(memoryUsageWin, m_pMemoryUsage.get(), m_elemsNumber);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to attach RMA window", __FILE__, __func__, __LINE__, mpiStatus);
        }
        MPI_Get_address(m_pMemoryUsage.get(), &m_memoryUsageBaseAddress);

        auto memoryWin = m_elemsWinWrapper.getMpiWin();
        T* pElems{nullptr};

        MPI_Type_size(m_datatype, &m_mpiElemSize);
        {
            auto mpiStatus = MPI_Alloc_mem(m_mpiElemSize * m_elemsNumber, MPI_INFO_NULL, &pElems);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to allocate RMA memory", __FILE__, __func__, __LINE__, mpiStatus);
        }
        m_pMemory = std::unique_ptr<T[], MemoryDeleter_t>(pElems, [logger=m_logger] (T* p) {
            MPI_Free_mem(p);
            SPDLOG_LOGGER_TRACE(logger, "freed up elements' memory");
        });
        {
            auto mpiStatus = MPI_Win_attach(memoryWin, m_pMemory.get(), m_mpiElemSize * m_elemsNumber);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to attach RMA window", __FILE__, __func__, __LINE__, mpiStatus);
        }
        MPI_Get_address(m_pMemory.get(), &m_elemsBaseAddress);
    }

    {
        auto mpiStatus = MPI_Bcast(&m_elemsBaseAddress, 1, MPI_AINT, CENTRAL_RANK, comm);
        if (mpiStatus != MPI_SUCCESS)
            throw custom_mpi::MpiException("failed to broadcast elements' base m_address", __FILE__, __func__ , __LINE__, mpiStatus);
    }
    {
        auto mpiStatus = MPI_Bcast(&m_mpiElemSize, 1, MPI_AINT, CENTRAL_RANK, comm);
        if (mpiStatus != MPI_SUCCESS)
            throw custom_mpi::MpiException("failed to broadcast element size", __FILE__, __func__ , __LINE__, mpiStatus);
    }
    {
        auto mpiStatus = MPI_Bcast(&m_memoryUsageBaseAddress, 1, MPI_AINT, CENTRAL_RANK, comm);
        if (mpiStatus != MPI_SUCCESS)
            throw custom_mpi::MpiException("failed to broadcast base elements' memory usage m_address", __FILE__, __func__ , __LINE__, mpiStatus);
    }

    SPDLOG_LOGGER_TRACE(m_logger, "elements' base m_address {}", m_elemsBaseAddress);
    SPDLOG_LOGGER_TRACE(m_logger, "elements' base memory usage m_address {}", m_memoryUsageBaseAddress);
}

template<typename T>
const custom_mpi::MpiWinWrapper &CentralizedMemoryPool<T>::getElemsWinWrapper() const 
{
    return m_elemsWinWrapper;
}

template<typename T>
MPI_Datatype CentralizedMemoryPool<T>::getDatatype() const {
    return m_datatype;
}

template<typename T>
int CentralizedMemoryPool<T>::getMpiDatatypeSize() const
{
    return m_mpiElemSize;
}

#endif //RMA_LIST_SKETCH_MEMORYPOOL_H
