//
// Created by denis on 09.04.23.
//

#include "CentralizedMemoryPool.h"

CentralizedMemoryPool::CentralizedMemoryPool(MPI_Comm comm, MPI_Info info, MPI_Datatype t_datatype,
                                             MPI_Aint t_elemsNumber, std::shared_ptr<spdlog::logger> t_logger)
:
        m_logger(std::move(t_logger)),
        m_elemMpiDatatype(t_datatype),
        m_elemsNumber(t_elemsNumber)
{
    {
        auto mpiStatus = MPI_Comm_rank(comm, &m_rank);
        if (mpiStatus != MPI_SUCCESS)
            throw custom_mpi::MpiException("failed to get rank", __FILE__, __func__, __LINE__, mpiStatus);
    }
    {
        auto mpiStatus = MPI_Win_create_dynamic(info, comm, &m_memoryUsageWin);
        if (mpiStatus != MPI_SUCCESS)
            throw custom_mpi::MpiException("failed to create RMA window for memory usage", __FILE__, __func__, __LINE__, mpiStatus);
    }
    {
        auto mpiStatus = MPI_Win_create_dynamic(info, comm, &m_elemsWin);
        if (mpiStatus != MPI_SUCCESS)
            throw custom_mpi::MpiException("failed to create RMA window for elements", __FILE__, __func__, __LINE__, mpiStatus);
    }
    allocateMemory(comm);
}

CentralizedMemoryPool &CentralizedMemoryPool::operator=(CentralizedMemoryPool &&t_other) noexcept
{
    if (&t_other != this)
    {
        m_pElems= std::exchange(t_other.m_pElems, nullptr);
        m_rank = std::exchange(t_other.m_rank, -1);
        m_elemsNumber = std::exchange(t_other.m_elemsNumber, 0);
        m_elemMpiDatatype = t_other.m_elemMpiDatatype;
        m_elemsBaseAddress = std::exchange(t_other.m_elemsBaseAddress, (MPI_Aint)MPI_BOTTOM);
        m_elemsWin = std::exchange(t_other.m_elemsWin, MPI_WIN_NULL);
        m_memoryUsageWin = std::exchange(t_other.m_memoryUsageWin, MPI_WIN_NULL);
    }
    return *this;
}

MPI_Aint CentralizedMemoryPool::allocate() const
{
    auto elemMemoryAddress{(MPI_Aint)MPI_BOTTOM};
    auto memoryUsageAddress{(MPI_Aint)MPI_BOTTOM};

    for (MPI_Aint disp = 0; disp < m_elemsNumber; ++disp)
    {
        MemoryUsageMark oldState{Released};
        MemoryUsageMark newState{Acquired};
        MemoryUsageMark resState{Acquired};

        memoryUsageAddress = MPI_Aint_add(m_memoryUsageBaseAddress, disp * m_sMemoryUsageMarkSize);

        SPDLOG_LOGGER_TRACE(m_logger, "trying to acquire memory usage m_address {}", memoryUsageAddress);

        MPI_Win_lock_all(0, m_memoryUsageWin);
        MPI_Compare_and_swap(&newState, &oldState, &resState, MPI_INT, CENTRAL_RANK, memoryUsageAddress, m_memoryUsageWin);
        MPI_Win_flush_all(m_memoryUsageWin);
        MPI_Win_unlock_all(m_memoryUsageWin);

        if (resState == MemoryUsageMark::Released)
        {
            elemMemoryAddress = MPI_Aint_add(m_elemsBaseAddress, disp * m_elemMpiDatatypeSize);
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


void CentralizedMemoryPool::deallocate(MPI_Aint elemMemoryAddress) const
{
    MemoryUsageMark oldState{Acquired};
    MemoryUsageMark newState{Released};
    MemoryUsageMark resState{Released};

    auto disp = MPI_Aint_diff(elemMemoryAddress, m_elemsBaseAddress) / m_elemMpiDatatypeSize;
    auto memoryUsageAddress = MPI_Aint_add(m_memoryUsageBaseAddress, disp * m_sMemoryUsageMarkSize);

    SPDLOG_LOGGER_TRACE(m_logger, "trying to release memory usage m_address {}", memoryUsageAddress);

    MPI_Win_lock_all(0, m_memoryUsageWin);
    MPI_Compare_and_swap(&newState, &oldState, &resState, m_elemMpiDatatype, CENTRAL_RANK, memoryUsageAddress, m_memoryUsageWin);
    MPI_Win_flush_all(m_memoryUsageWin);
    MPI_Win_unlock_all(m_memoryUsageWin);

    if (resState != MemoryUsageMark::Acquired)
    {
        SPDLOG_LOGGER_TRACE(m_logger, "invalid memory usage m_address mark {} to memory usage m_address {}", resState, memoryUsageAddress);
        assert(resState == MemoryUsageMark::Acquired);
    }

    SPDLOG_LOGGER_TRACE(m_logger, "released memory usage m_address {}", memoryUsageAddress);
    SPDLOG_LOGGER_TRACE(m_logger, "released element memory m_address {}", elemMemoryAddress);
}


bool CentralizedMemoryPool::isCentralRank() const
{
    return m_rank == CENTRAL_RANK;
}


CentralizedMemoryPool::CentralizedMemoryPool(CentralizedMemoryPool &&t_other) noexcept
:
        m_pElems(std::exchange(t_other.m_pElems, nullptr)),
        m_pMemoryUsage(std::exchange(t_other.m_pMemoryUsage, nullptr)),
        m_rank(std::exchange(t_other.m_rank, -1)),
        m_elemsNumber(std::exchange(t_other.m_elemsNumber, 0)),
        m_elemMpiDatatype(t_other.m_elemMpiDatatype),
        m_elemsBaseAddress(std::exchange(t_other.m_elemsBaseAddress, (MPI_Aint)MPI_BOTTOM)),
        m_elemsWin(std::exchange(t_other.m_elemsWin, MPI_WIN_NULL)),
        m_memoryUsageWin(std::exchange(t_other.m_memoryUsageWin, MPI_WIN_NULL))
{

}

void CentralizedMemoryPool::allocateMemory(MPI_Comm comm)
{
    if (isCentralRank())
    {
        {
            auto mpiStatus = MPI_Alloc_mem(sizeof(MemoryUsageMark) * m_elemsNumber, MPI_INFO_NULL, &m_pMemoryUsage);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to allocate RMA memory", __FILE__, __func__, __LINE__,
                                               mpiStatus);
        }
        std::fill_n(m_pMemoryUsage, m_elemsNumber, MemoryUsageMark::Released);
        {
            auto mpiStatus = MPI_Win_attach(m_memoryUsageWin, m_pMemoryUsage, m_elemsNumber);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to attach RMA window", __FILE__, __func__, __LINE__, mpiStatus);
        }
        MPI_Get_address(m_pMemoryUsage, &m_memoryUsageBaseAddress);

        MPI_Type_size(m_elemMpiDatatype, &m_elemMpiDatatypeSize);
        {
            auto mpiStatus = MPI_Alloc_mem(m_elemMpiDatatypeSize * m_elemsNumber, MPI_INFO_NULL, &m_pElems);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to allocate RMA memory", __FILE__, __func__, __LINE__, mpiStatus);
        }

        {
            auto mpiStatus = MPI_Win_attach(m_elemsWin, m_pElems, m_elemMpiDatatypeSize * m_elemsNumber);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to attach RMA window", __FILE__, __func__, __LINE__, mpiStatus);
        }
        MPI_Get_address(m_pElems, &m_elemsBaseAddress);
    }

    {
        auto mpiStatus = MPI_Bcast(&m_elemsBaseAddress, 1, MPI_AINT, CENTRAL_RANK, comm);
        if (mpiStatus != MPI_SUCCESS)
            throw custom_mpi::MpiException("failed to broadcast elements' base m_address", __FILE__, __func__ , __LINE__, mpiStatus);
    }
    {
        auto mpiStatus = MPI_Bcast(&m_elemMpiDatatypeSize, 1, MPI_AINT, CENTRAL_RANK, comm);
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

MPI_Datatype CentralizedMemoryPool::getElemMpiDatatype() const {
    return m_elemMpiDatatype;
}

int CentralizedMemoryPool::getMpiDatatypeSize() const
{
    return m_elemMpiDatatypeSize;
}

const MPI_Win &CentralizedMemoryPool::getElemsWin() const {
    return m_elemsWin;
}

void CentralizedMemoryPool::release()
{
    MPI_Free_mem(m_pMemoryUsage);
    m_pMemoryUsage = nullptr;
    SPDLOG_LOGGER_TRACE(logger, "freed up memory usage RMA memory");

    MPI_Win_free(&m_memoryUsageWin);
    SPDLOG_LOGGER_TRACE(logger, "freed up memory usage RMA window");

    MPI_Free_mem(m_pElems);
    m_pElems = nullptr;
    SPDLOG_LOGGER_TRACE(logger, "freed up elements' RMA memory");

    MPI_Win_free(&m_elemsWin);
    SPDLOG_LOGGER_TRACE(logger, "freed up elements' RMA window");
}
