//
// Created by denis on 18.03.23.
//

#ifndef RMA_LIST_SKETCH_MEMORYPOOL_H
#define RMA_LIST_SKETCH_MEMORYPOOL_H

#include <mpi.h>
#include <memory>
#include <cassert>
#include <spdlog/spdlog.h>

#include "MpiException.h"
#include "rma.h"
#include "memory_pool.h"
using namespace std::literals::string_literals;

namespace custom_mpi = custom_mpi_extensions;


class CentralizedMemoryPool
{
    static constexpr auto m_sMemoryUsageMarkSize = static_cast<MPI_Aint>(sizeof(MemoryUsageMark));

    constexpr inline static int CENTRAL_RANK{0};
public:

    CentralizedMemoryPool(MPI_Comm comm, MPI_Info info, MPI_Datatype t_datatype,
                          MPI_Aint t_elemsNumber, std::shared_ptr<spdlog::logger> t_logger);
    ~CentralizedMemoryPool() = default;
    CentralizedMemoryPool(CentralizedMemoryPool&) = delete;
    CentralizedMemoryPool(CentralizedMemoryPool&& t_other) noexcept;
    CentralizedMemoryPool& operator=(CentralizedMemoryPool&) = delete;
    CentralizedMemoryPool& operator=(CentralizedMemoryPool&& t_other) noexcept;

    [[nodiscard]] MPI_Aint allocate() const;
    void deallocate(MPI_Aint elemMemoryAddress) const;
    void release();

    [[nodiscard]] const MPI_Win & getElemsWin() const;
    [[nodiscard]] MPI_Datatype getElemMpiDatatype() const;
    [[nodiscard]] int getMpiDatatypeSize() const;

private:
    void allocateMemory(MPI_Comm comm);
    [[nodiscard]] bool isCentralRank() const;

private:
    std::shared_ptr<spdlog::logger> m_logger;

    void* m_pElems{nullptr};
    MemoryUsageMark* m_pMemoryUsage{nullptr};
    int m_rank{-1};
    int m_elemMpiDatatypeSize{0};
    MPI_Aint m_elemsNumber{0};
    MPI_Datatype m_elemMpiDatatype{MPI_DATATYPE_NULL};
    MPI_Aint m_elemsBaseAddress{(MPI_Aint) MPI_BOTTOM};
    MPI_Aint m_memoryUsageBaseAddress{(MPI_Aint) MPI_BOTTOM};
    MPI_Win m_elemsWin{MPI_WIN_NULL};
    MPI_Win m_memoryUsageWin{MPI_WIN_NULL};
};

#endif //RMA_LIST_SKETCH_MEMORYPOOL_H
