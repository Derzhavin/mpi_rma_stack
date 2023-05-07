//
// Created by denis on 07.05.23.
//

#ifndef SOURCES_RMATREIBERDECENTRALIZEDSTACK_H
#define SOURCES_RMATREIBERDECENTRALIZEDSTACK_H

#include <mpi.h>
#include <memory>
#include <optional>

#include "IStack.h"

#include "outer/Backoff.h"
#include "inner/InnerStack.h"
#include "MpiException.h"

namespace rma_stack
{
    namespace custom_mpi = custom_mpi_extensions;

    template<typename T>
    class RmaTreiberDecentralizedStack: public stack_interface::IStack<RmaTreiberDecentralizedStack<T>>
    {
        static constexpr inline int CENTRAL_RANK{0};
        friend class stack_interface::IStack_traits<rma_stack::RmaTreiberDecentralizedStack<T>>;
    public:
        typedef typename stack_interface::IStack_traits<RmaTreiberDecentralizedStack>::ValueType ValueType;

        explicit RmaTreiberDecentralizedStack(MPI_Comm comm, const std::chrono::nanoseconds &t_rBackoffMinDelay,
                                        const std::chrono::nanoseconds &t_rBackoffMaxDelay,
                                        ref_counting::InnerStack &&t_innerStack,
                                        std::shared_ptr<spdlog::logger> t_logger);
        static RmaTreiberDecentralizedStack <T> create(
                MPI_Comm comm,
                MPI_Info info,
                const std::chrono::nanoseconds &t_rBackoffMinDelay,
                const std::chrono::nanoseconds &t_rBackoffMaxDelay,
                int elemsUpLimit,
                std::shared_ptr<spdlog::sinks::sink> logger_sink
        );

        RmaTreiberDecentralizedStack(RmaTreiberDecentralizedStack&) = delete;
        RmaTreiberDecentralizedStack(RmaTreiberDecentralizedStack&&)  noexcept = default;
        RmaTreiberDecentralizedStack& operator=(RmaTreiberDecentralizedStack&) = delete;
        RmaTreiberDecentralizedStack& operator=(RmaTreiberDecentralizedStack&&)  noexcept = default;
        ~RmaTreiberDecentralizedStack() = default;

        void release();

    private:
        // public stack interface begin
        void pushImpl(const T &rValue);
        T popImpl();
        T& topImpl();
        size_t sizeImpl();
        bool isEmptyImpl();
        // public stack interface end

        void allocateProprietaryData(MPI_Comm comm);
        bool isStopRequested();
        bool tryPush(const T &rValue);
        bool tryPop(T &rValue);

    private:
        std::chrono::nanoseconds backoffMinDelay;
        std::chrono::nanoseconds backoffMaxDelay;

        ref_counting::InnerStack m_innerStack;
        int m_rank{-1};
        MPI_Win m_dataWin{MPI_WIN_NULL};
        T* m_pDataArr{nullptr};
        std::unique_ptr<MPI_Aint[]> m_pDataBaseAddresses;
        std::shared_ptr<spdlog::logger> m_logger;
    };

    template<typename T>
    void RmaTreiberDecentralizedStack<T>::release()
    {
        while (!popImpl());
        m_innerStack.release();

        MPI_Free_mem(m_pDataArr);
        m_pDataArr = nullptr;
        SPDLOG_LOGGER_TRACE(m_logger, "freed up data arr RMA memory");

        MPI_Win_free(&m_dataWin);
        SPDLOG_LOGGER_TRACE(m_logger, "freed up data win RMA memory");
    }

    template<typename T>
    RmaTreiberDecentralizedStack<T>::RmaTreiberDecentralizedStack(MPI_Comm comm, const std::chrono::nanoseconds &t_rBackoffMinDelay,
                                                      const std::chrono::nanoseconds &t_rBackoffMaxDelay,
                                                      ref_counting::InnerStack &&t_innerStack,
                                                      std::shared_ptr<spdlog::logger> t_logger)
            :
            backoffMinDelay(t_rBackoffMinDelay),
            backoffMaxDelay(t_rBackoffMaxDelay),
            m_innerStack(std::move(t_innerStack)),
            m_logger(std::move(t_logger))
    {
        MPI_Comm_rank(comm, &m_rank);

        allocateProprietaryData(comm);
    }

    template<typename T>
    void RmaTreiberDecentralizedStack<T>::pushImpl(const T &rValue)
    {
        Backoff backoff(backoffMinDelay, backoffMaxDelay);
        while(!isStopRequested())
        {
            if (tryPush(rValue))
            {
                return;
            }
            else
            {
                backoff.backoff();
            }
        }
        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d finished 'pushImpl'",
                            m_rank
        );
    }

    template<typename T>
    T RmaTreiberDecentralizedStack<T>::popImpl()
    {
        Backoff backoff(backoffMinDelay, backoffMaxDelay);

        T value{};

        while (!isStopRequested())
        {
            if (tryPop(value))
            {
                break;
            }
            else
            {
                backoff.backoff();
            }
        }
        return value;
    }

    template<typename T>
    T &rma_stack::RmaTreiberDecentralizedStack<T>::topImpl() {
        T v{};
        return v;
    }

    template<typename T>
    size_t RmaTreiberDecentralizedStack<T>::sizeImpl()
    {
        return 0;
    }

    template<typename T>
    bool RmaTreiberDecentralizedStack<T>::isEmptyImpl()
    {
        return true;
    }

    template<typename T>
    bool RmaTreiberDecentralizedStack<T>::isStopRequested()
    {
        return false;
    }

    template<typename T>
    bool RmaTreiberDecentralizedStack<T>::tryPush(const T &rValue)
    {
        auto res = m_innerStack.push([&rValue, &win = m_dataWin, &pDataBaseAddresses=m_pDataBaseAddresses](const ref_counting::GlobalAddress &dataAddress) {
            constexpr auto valueSize    = sizeof(rValue);
            const auto offset           = dataAddress.offset * valueSize;
            const auto displacement     = MPI_Aint_add(pDataBaseAddresses[dataAddress.rank], offset);
            MPI_Win_lock(MPI_LOCK_SHARED, dataAddress.rank, 0, win);
            MPI_Put(&rValue,
                    valueSize,
                    MPI_UNSIGNED_CHAR,
                    dataAddress.rank,
                    displacement,
                    valueSize,
                    MPI_UNSIGNED_CHAR,
                    win
            );
            MPI_Win_flush(dataAddress.rank, win);
            MPI_Win_unlock(dataAddress.rank, win);
        });

        if (!res)
            return false;

        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d finished 'tryPush'",
                            m_rank
        );
        return true;
    }

    template<typename T>
    bool RmaTreiberDecentralizedStack<T>::tryPop(T &rValue)
    {
        m_innerStack.pop([&rValue, &win=m_dataWin, &pDataBaseAddresses=m_pDataBaseAddresses](const ref_counting::GlobalAddress &dataAddress) {
            if (ref_counting::isGlobalAddressDummy(dataAddress))
                return;

            constexpr auto valueSize    = sizeof(rValue);
            const auto offset           = dataAddress.offset * valueSize;
            const auto displacement     = MPI_Aint_add(pDataBaseAddresses[dataAddress.rank], offset);
            MPI_Win_lock(MPI_LOCK_SHARED, dataAddress.rank, 0, win);
            MPI_Get(&rValue,
                    valueSize,
                    MPI_UNSIGNED_CHAR,
                    dataAddress.rank,
                    displacement,
                    valueSize,
                    MPI_UNSIGNED_CHAR,
                    win
            );
            MPI_Win_flush(dataAddress.rank, win);
            MPI_Win_unlock(dataAddress.rank, win);
        });
        return true;
    }

    template<typename T>
    void RmaTreiberDecentralizedStack<T>::allocateProprietaryData(MPI_Comm comm)
    {
        auto elemsUpLimit = m_innerStack.getElemsUpLimit();
        constexpr auto elemSize = sizeof(T);
        {
            auto mpiStatus = MPI_Alloc_mem(elemSize * elemsUpLimit, MPI_INFO_NULL,
                                           &m_pDataArr);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException(
                        "failed to allocate RMA memory",
                        __FILE__,
                        __func__,
                        __LINE__,
                        mpiStatus
                );
        }
        std::fill_n(m_pDataArr, elemsUpLimit, T());
        {
            auto mpiStatus = MPI_Win_attach(m_dataWin, m_pDataArr, elemsUpLimit);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to attach RMA window", __FILE__, __func__, __LINE__, mpiStatus);
        }
        int procNum{0};
        MPI_Comm_size(comm, &procNum);
        m_pDataBaseAddresses = std::make_unique<MPI_Aint[]>(procNum);
        MPI_Get_address(m_pDataArr, &m_pDataBaseAddresses[m_rank]);

        for (int i = 0; i < procNum; ++i)
        {
            if (i == m_rank)
            {
                auto mpiStatus = MPI_Bcast(&m_pDataBaseAddresses[i], 1, MPI_AINT, m_rank, comm);
                if (mpiStatus != MPI_SUCCESS)
                    throw custom_mpi::MpiException("failed to broadcast data array base address", __FILE__, __func__ , __LINE__, mpiStatus);
            }
        }
    }

    template<typename T>
    RmaTreiberDecentralizedStack<T> RmaTreiberDecentralizedStack<T>::create(MPI_Comm comm, MPI_Info info,
                                                                const std::chrono::nanoseconds &t_rBackoffMinDelay,
                                                                const std::chrono::nanoseconds &t_rBackoffMaxDelay,
                                                                int elemsUpLimit,
                                                                std::shared_ptr<spdlog::sinks::sink> logger_sink) {
        auto pInnerStackLogger = std::make_shared<spdlog::logger>("InnerStack", logger_sink);
        spdlog::register_logger(pInnerStackLogger);
        pInnerStackLogger->set_level(static_cast<spdlog::level::level_enum>(SPDLOG_ACTIVE_LEVEL));
        pInnerStackLogger->flush_on(static_cast<spdlog::level::level_enum>(SPDLOG_ACTIVE_LEVEL));

        ref_counting::InnerStack innerStack(
                comm,
                info,
                CENTRAL_RANK,
                false,
                elemsUpLimit,
                std::move(pInnerStackLogger)
        );

        RmaTreiberDecentralizedStack<T> stack(
                comm,
                t_rBackoffMinDelay,
                t_rBackoffMaxDelay,
                std::move(innerStack), std::shared_ptr<spdlog::logger>());

        return stack;
    }
} // rma_stack


namespace stack_interface
{
    template<typename T>
    struct IStack_traits<rma_stack::RmaTreiberDecentralizedStack < T>>
    {
        friend class IStack<rma_stack::RmaTreiberDecentralizedStack < T>>;
        friend class rma_stack::RmaTreiberDecentralizedStack<T>;
        typedef T ValueType;

    private:
        static void pushImpl(rma_stack::RmaTreiberDecentralizedStack<T>& stack, const T &value)
        {
            stack.pushImpl(value);
        }
        static ValueType popImpl(rma_stack::RmaTreiberDecentralizedStack<T>& stack)
        {
            return stack.popImpl();
        }
        static ValueType& topImpl(rma_stack::RmaTreiberDecentralizedStack<T>& stack)
        {
            return stack.topImpl();
        }
        static size_t sizeImpl(rma_stack::RmaTreiberDecentralizedStack<T>& stack)
        {
            return stack.sizeImpl();
        }
        static bool isEmptyImpl(rma_stack::RmaTreiberDecentralizedStack<T>& stack)
        {
            return stack.isEmptyImpl();
        }
    };
}
#endif //SOURCES_RMATREIBERDECENTRALIZEDSTACK_H
