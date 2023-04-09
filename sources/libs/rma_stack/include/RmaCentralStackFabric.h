//
// Created by denis on 04.04.23.
//

#ifndef SOURCES_RMACENTRALSTACKFABRIC_H
#define SOURCES_RMACENTRALSTACKFABRIC_H

#include "RmaTreiberCentralStack.h"

namespace rma_stack
{
    class RmaCentralStackFabric
    {
    public:
        static RmaCentralStackFabric& getInstance();

        template<typename T>
        RmaTreiberCentralStack<T> createRmaTreiberCentralStack(MPI_Comm comm,
                                                            MPI_Info info,
                                                            MPI_Datatype t_mpiDataType,
                                                            const std::chrono::nanoseconds &t_rBackoffMinDelay,
                                                            const std::chrono::nanoseconds &t_rBackoffMaxDelay,
                                                            int t_freeNodesLimit,
                                                            MPI_Aint memoryPoolSize,
                                                            std::shared_ptr<spdlog::sinks::sink> logger_sink);
        void finalize();

        RmaCentralStackFabric(RmaCentralStackFabric&) = delete;
        RmaCentralStackFabric& operator=(RmaCentralStackFabric&) = delete;
        ~RmaCentralStackFabric() = default;

    private:
        RmaCentralStackFabric();
        RmaCentralStackFabric(RmaCentralStackFabric&&) = default;
        RmaCentralStackFabric& operator=(RmaCentralStackFabric&&) = default;

        void registerCountedNodeMpiDatatype();
        void releaseCountedNodeMpiDatatype();
        void registerCentralNodeMpiDatatype();
        void releaseCentralNodeMpiDatatype();
        [[nodiscard]] bool isCountedNodeMpiDatatypeRegistered() const;
        [[nodiscard]] bool isCentralNodeMpiDatatypeRegistered() const;

    private:
        MPI_Datatype m_countedNodeMpiDatatype{MPI_DATATYPE_NULL};
        MPI_Datatype m_centralNodeMpiDatatype{MPI_DATATYPE_NULL};
    };

    template<typename T>
    RmaTreiberCentralStack<T>
    RmaCentralStackFabric::createRmaTreiberCentralStack(MPI_Comm comm,
                                                        MPI_Info info,
                                                        MPI_Datatype t_mpiDataType,
                                                        const std::chrono::nanoseconds &t_rBackoffMinDelay,
                                                        const std::chrono::nanoseconds &t_rBackoffMaxDelay,
                                                        int t_freeNodesLimit,
                                                        MPI_Aint memoryPoolSize,
                                                        std::shared_ptr<spdlog::sinks::sink> logger_sink)
    {
        auto pElemsPoolLogger = std::make_shared<spdlog::logger>("ElemsPoolLogger", logger_sink);
        spdlog::register_logger(pElemsPoolLogger);
        pElemsPoolLogger->set_level(static_cast<spdlog::level::level_enum>(SPDLOG_ACTIVE_LEVEL));
        pElemsPoolLogger->flush_on(static_cast<spdlog::level::level_enum>(SPDLOG_ACTIVE_LEVEL));

        CentralizedMemoryPool elemsMemoryPool(
                comm,
                info,
                m_centralNodeMpiDatatype,
                memoryPoolSize,
                std::move(pElemsPoolLogger)
        );

        auto pCountedNodesPoolLogger = std::make_shared<spdlog::logger>("CountedNodesPoolLogger", logger_sink);
        spdlog::register_logger(pCountedNodesPoolLogger);
        pCountedNodesPoolLogger->set_level(static_cast<spdlog::level::level_enum>(SPDLOG_ACTIVE_LEVEL));
        pCountedNodesPoolLogger->flush_on(static_cast<spdlog::level::level_enum>(SPDLOG_ACTIVE_LEVEL));

        CentralizedMemoryPool countedNodesMemoryPool(
                comm,
                info,
                m_countedNodeMpiDatatype,
                memoryPoolSize,
                std::move(pCountedNodesPoolLogger)
        );

        auto pCentralNodesPoolLogger = std::make_shared<spdlog::logger>("CentralNodesPoolLogger", logger_sink);
        spdlog::register_logger(pCentralNodesPoolLogger);
        pCentralNodesPoolLogger->set_level(static_cast<spdlog::level::level_enum>(SPDLOG_ACTIVE_LEVEL));
        pCentralNodesPoolLogger->flush_on(static_cast<spdlog::level::level_enum>(SPDLOG_ACTIVE_LEVEL));

        CentralizedMemoryPool centralNodesMemoryPool(
                comm,
                info,
                m_centralNodeMpiDatatype,
                memoryPoolSize,
                std::move(pCentralNodesPoolLogger)
        );

        RmaTreiberCentralStack<T> stack(
                comm,
                t_rBackoffMinDelay,
                t_rBackoffMaxDelay,
                t_freeNodesLimit,
                std::move(elemsMemoryPool),
                std::move(countedNodesMemoryPool),
                std::move(centralNodesMemoryPool)
        );

        return stack;
    }
} // rma_stack

#endif //SOURCES_RMACENTRALSTACKFABRIC_H
