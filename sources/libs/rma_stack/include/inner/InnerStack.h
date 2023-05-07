//
// Created by denis on 20.04.23.
//

#ifndef SOURCES_INNERSTACK_H
#define SOURCES_INNERSTACK_H

#include <utility>
#include <cstddef>
#include <mpi.h>
#include <spdlog/spdlog.h>
#include <functional>

#include "CountedNodePtr.h"
#include "Node.h"

namespace rma_stack::ref_counting
{
        class InnerStack
        {
            constexpr inline static int CENTRAL_RANK{0};

        public:
            InnerStack(MPI_Comm comm,
                       MPI_Info info,
                       size_t t_elemsUpLimit,
                       std::shared_ptr<spdlog::logger> t_logger
           );
            bool push(const std::function<void(GlobalAddress)>& putDataCallback);
            void pop(const std::function<void(GlobalAddress)>& getDataCallback);
            void release();
            [[nodiscard]] size_t getElemsUpLimit() const;

        private:
            [[nodiscard]] bool isCentralRank() const;
            void allocateProprietaryData(MPI_Comm comm);
            void increaseHeadCount(CountedNodePtr& oldCountedNodePtr);
            void initStackWithDummy();
            [[nodiscard]] GlobalAddress acquireNode(int rank) const;

            void releaseNode(GlobalAddress nodeAddress) const;
        private:
            size_t m_elemsUpLimit{0};
            int m_rank{-1};

            MPI_Win m_headWin{MPI_WIN_NULL};
            CountedNodePtr* m_pHeadCountedNodePtr{nullptr};
            GlobalAddress m_headAddress{.offset = 0, .rank = DummyRank};

            MPI_Win m_nodeWin{MPI_WIN_NULL};
            Node* m_pNodeArr{nullptr};
            MPI_Aint m_nodeArrBase{(MPI_Aint) MPI_BOTTOM};

            std::shared_ptr<spdlog::logger> m_logger;
        };

    } // ref_counting

#endif //SOURCES_INNERSTACK_H
