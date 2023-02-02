//
// Created by denis on 02.02.23.
//

#include "include/MpiException.hpp"

namespace cutom_mpi_extensions
{
    MpiException::MpiException(const char *t_info, const char *t_file, const char *t_rknnFunctionName, int t_line,
                               int t_mpiError)
            :
            std::exception(),
            m_info(t_info),
            m_file(t_file),
            m_line(t_line),
            m_mpiError(t_mpiError),
            m_rknnFunctionName(t_rknnFunctionName)
    {

    }

    const char * MpiException::what () const noexcept
    {
        return "MpiException";
    }

    std::string_view MpiException::getInfo()
    {
        return m_info;
    }
    std::string_view MpiException::getFile()
    {
        return m_file;
    }
    int MpiException::getLine()
    {
        return m_line;
    }

    int MpiException::getMpiError()
    {
        return m_mpiError;
    }
}