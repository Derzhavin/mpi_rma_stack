//
// Created by denis on 20.04.23.
//

#ifndef SOURCES_INNERSTACK_H
#define SOURCES_INNERSTACK_H

#include <utility>
#include <cstddef>
#include <mpi.h>
#include <spdlog/spdlog.h>

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
            DataAddress allocatePush(uint64_t rank);
            DataAddress deallocatePop();
            void release();

        private:
            [[nodiscard]] bool isCentralRank() const;
            void allocateProprietaryData(MPI_Comm comm);
            void increaseHeadCount(CountedNodePtr& oldCounter);
            [[nodiscard]] DataAddress acquireNode(uint64_t rank) const;
            void releaseNode(DataAddress nodeAddress) const;
            void initStackWithDummy();
        private:
            size_t m_elemsUpLimit{0};
        public:
            [[nodiscard]] size_t getElemsUpLimit() const;

        private:
            int m_rank{-1};

            MPI_Win m_headWin{MPI_WIN_NULL};
            CountedNodePtr* m_pHeadCountedNodePtr{nullptr};
            DataAddress m_headAddress{.offset = 0, .rank = CountedNodePtrDummyRank};

            MPI_Win m_nodeWin{MPI_WIN_NULL};
            Node* m_pNodeArr{nullptr};
            MPI_Aint m_nodeArrBaseAddress{(MPI_Aint) MPI_BOTTOM};

            std::shared_ptr<spdlog::logger> m_logger;
        };

    } // ref_counting

#endif //SOURCES_INNERSTACK_H
