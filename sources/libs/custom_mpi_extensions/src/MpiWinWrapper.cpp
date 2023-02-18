//
// Created by denis on 12.02.23.
//

#include <utility>

#include "include/MpiWinWrapper.h"
#include "include/MpiException.h"

namespace custom_mpi_extensions
{
    MpiWinWrapper::MpiWinWrapper(MPI_Info info, MPI_Comm comm)
    {
        auto mpiStatus = MPI_Win_create_dynamic(info, comm, &m_mpiWin);
        if (mpiStatus != MPI_SUCCESS)
            throw MpiException("failed to create RMA window", __FILE__, __func__, __LINE__, mpiStatus);
    }

    const MPI_Win& MpiWinWrapper::getMpiWin() const
    {
        return m_mpiWin;
    }

    MpiWinWrapper::~MpiWinWrapper()
    {
        if (m_mpiWin != MPI_WIN_NULL)
            MPI_Win_free(&m_mpiWin);
    }

    MpiWinWrapper::MpiWinWrapper(MpiWinWrapper &&other) noexcept
    :
    m_mpiWin(std::exchange(other.m_mpiWin, MPI_WIN_NULL))
    {

    }

    MpiWinWrapper &MpiWinWrapper::operator=(MpiWinWrapper &&other) noexcept
    {
        if (&other != this)
            m_mpiWin = std::exchange(other.m_mpiWin, MPI_WIN_NULL);
        return *this;
    }
}