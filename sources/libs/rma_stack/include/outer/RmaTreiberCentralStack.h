//
// Created by denis on 22.01.23.
//

#ifndef SOURCES_RMATREIBERCENTRALSTACK_H
#define SOURCES_RMATREIBERCENTRALSTACK_H

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
    class RmaTreiberCentralStack: public stack_interface::IStack<RmaTreiberCentralStack<T>>
    {
        friend class stack_interface::IStack_traits<rma_stack::RmaTreiberCentralStack<T>>;
    public:
        typedef typename stack_interface::IStack_traits<RmaTreiberCentralStack>::ValueType ValueType;

        explicit RmaTreiberCentralStack(MPI_Comm comm, const std::chrono::nanoseconds &t_rBackoffMinDelay,
                                        const std::chrono::nanoseconds &t_rBackoffMaxDelay,
                                        ref_counting::InnerStack &&t_innerStack,
                                        std::shared_ptr<spdlog::logger> t_logger);
        static RmaTreiberCentralStack <T> create(
                MPI_Comm comm,
                MPI_Info info,
                const std::chrono::nanoseconds &t_rBackoffMinDelay,
                const std::chrono::nanoseconds &t_rBackoffMaxDelay,
                int elemsUpLimit,
                std::shared_ptr<spdlog::sinks::sink> logger_sink
        );

        RmaTreiberCentralStack(RmaTreiberCentralStack&) = delete;
        RmaTreiberCentralStack(RmaTreiberCentralStack&&)  noexcept = default;
        RmaTreiberCentralStack& operator=(RmaTreiberCentralStack&) = delete;
        RmaTreiberCentralStack& operator=(RmaTreiberCentralStack&&)  noexcept = default;
        ~RmaTreiberCentralStack() = default;

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
        void putDataByAddress(const T &rValue, ref_counting::GlobalAddress &dataAddress);
        void getDataByAddress(const T &rValue, ref_counting::GlobalAddress &dataAddress);

    private:
        std::chrono::nanoseconds backoffMinDelay;
        std::chrono::nanoseconds backoffMaxDelay;

        ref_counting::InnerStack m_innerStack;
        int m_rank{-1};
        MPI_Win m_dataWin{MPI_WIN_NULL};
        T* m_pDataArr{nullptr};
        std::shared_ptr<spdlog::logger> m_logger;
    };

    template<typename T>
    void RmaTreiberCentralStack<T>::getDataByAddress(const T &rValue, ref_counting::GlobalAddress &dataAddress)
    {
        constexpr auto valueSize = sizeof (rValue);
        MPI_Win_lock_all(0, m_dataWin);
        MPI_Get(&rValue,
                valueSize,
                MPI_UNSIGNED_CHAR,
                dataAddress.rank,
                dataAddress.offset,
                valueSize,
                MPI_UNSIGNED_CHAR,
                m_dataWin
        );
        MPI_Win_flush(dataAddress.rank, m_dataWin);
        MPI_Win_unlock_all(m_dataWin);
    }

    template<typename T>
    void RmaTreiberCentralStack<T>::putDataByAddress(const T &rValue, ref_counting::GlobalAddress &dataAddress)
    {
        constexpr auto valueSize = sizeof (rValue);
        MPI_Win_lock_all(0, m_dataWin);
        MPI_Put(&rValue,
                valueSize,
                MPI_UNSIGNED_CHAR,
                dataAddress.rank,
                dataAddress.offset,
                valueSize,
                MPI_UNSIGNED_CHAR,
                m_dataWin
        );
        MPI_Win_flush(dataAddress.rank, m_dataWin);
        MPI_Win_unlock_all(m_dataWin);
    }

    template<typename T>
    void RmaTreiberCentralStack<T>::release()
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
    RmaTreiberCentralStack<T>::RmaTreiberCentralStack(MPI_Comm comm, const std::chrono::nanoseconds &t_rBackoffMinDelay,
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
    void RmaTreiberCentralStack<T>::pushImpl(const T &rValue)
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
    T RmaTreiberCentralStack<T>::popImpl()
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
    T &rma_stack::RmaTreiberCentralStack<T>::topImpl() {
        T v{};
        return v;
    }

    template<typename T>
    size_t RmaTreiberCentralStack<T>::sizeImpl()
    {
        return 0;
    }

    template<typename T>
    bool RmaTreiberCentralStack<T>::isEmptyImpl()
    {
        return true;
    }

    template<typename T>
    bool RmaTreiberCentralStack<T>::isStopRequested()
    {
        return false;
    }

    template<typename T>
    bool RmaTreiberCentralStack<T>::tryPush(const T &rValue)
    {
        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d started 'tryPush'",
                            m_rank
        );

        auto nodeAddress = m_innerStack.acquireNode(m_rank);
        if (isGlobalAddressDummy(nodeAddress))
        {
            SPDLOG_LOGGER_TRACE(m_logger,
                                "rank %d failed to find free node in 'tryPush'",
                                m_rank
            );
            return false;
        }
        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d acquired free node in 'tryPush'",
                            m_rank
        );

        ref_counting::GlobalAddress dataAddress = nodeAddress;
        m_innerStack.putDataAddressInNode(nodeAddress, dataAddress);
        putDataByAddress(rValue, dataAddress);
        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d put data in 'tryPush'",
                            m_rank
        );

        m_innerStack.push(nodeAddress);

        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d finished 'tryPush'",
                            m_rank
        );
        return true;
    }

    template<typename T>
    bool RmaTreiberCentralStack<T>::tryPop(T &rValue)
    {
        ref_counting::GlobalAddress nodeAddress = m_innerStack.popHalf();

        if (ref_counting::isGlobalAddressDummy(nodeAddress))
            return false;

        ref_counting::GlobalAddress dataAddress = nodeAddress;
        getDataByAddress(rValue, dataAddress);

        m_innerStack.releaseNode(nodeAddress);
        return true;
    }

    template<typename T>
    void RmaTreiberCentralStack<T>::allocateProprietaryData(MPI_Comm comm)
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
    }

    template<typename T>
    RmaTreiberCentralStack<T> RmaTreiberCentralStack<T>::create(MPI_Comm comm, MPI_Info info,
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
                elemsUpLimit,
                std::move(pInnerStackLogger)
        );

        RmaTreiberCentralStack<T> stack(
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
    struct IStack_traits<rma_stack::RmaTreiberCentralStack < T>>
    {
        friend class IStack<rma_stack::RmaTreiberCentralStack < T>>;
        friend class rma_stack::RmaTreiberCentralStack<T>;
        typedef T ValueType;

    private:
        static void pushImpl(rma_stack::RmaTreiberCentralStack<T>& stack, const T &value)
        {
            stack.pushImpl(value);
        }
        static ValueType popImpl(rma_stack::RmaTreiberCentralStack<T>& stack)
        {
            return stack.popImpl();
        }
        static ValueType& topImpl(rma_stack::RmaTreiberCentralStack<T>& stack)
        {
            return stack.topImpl();
        }
        static size_t sizeImpl(rma_stack::RmaTreiberCentralStack<T>& stack)
        {
            return stack.sizeImpl();
        }
        static bool isEmptyImpl(rma_stack::RmaTreiberCentralStack<T>& stack)
        {
            return stack.isEmptyImpl();
        }
    };
}
#endif //SOURCES_RMATREIBERCENTRALSTACK_H
