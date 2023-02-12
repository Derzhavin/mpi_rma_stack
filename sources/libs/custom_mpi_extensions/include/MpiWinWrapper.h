//
// Created by denis on 12.02.23.
//

#ifndef SOURCES_MPIWINWRAPPER_H
#define SOURCES_MPIWINWRAPPER_H

#include <mpi.h>

namespace custom_mpi_extensions
{
    class MpiWinWrapper
    {
    public:
        explicit MpiWinWrapper(MPI_Info info, MPI_Comm comm);
        explicit MpiWinWrapper() = default;
        MpiWinWrapper(MpiWinWrapper&) = delete;
        MpiWinWrapper(MpiWinWrapper&& other) noexcept;
        MpiWinWrapper& operator=(MpiWinWrapper&) = delete;
        MpiWinWrapper& operator=(MpiWinWrapper&& other) noexcept;

        [[nodiscard]] const MPI_Win& getMpiWin() const;

        virtual ~MpiWinWrapper();

    private:
        MPI_Win m_mpiWin{MPI_WIN_NULL};
    };
}

#endif //SOURCES_MPIWINWRAPPER_H
