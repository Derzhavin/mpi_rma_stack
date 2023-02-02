//
// Created by denis on 02.02.23.
//

#ifndef SOURCES_MPIEXCEPTION_HPP
#define SOURCES_MPIEXCEPTION_HPP


#include <exception>
#include <string_view>
#include <string>

namespace cutom_mpi_extensions
{
    class MpiException: public std::exception
    {
    public:
        MpiException(const char *t_info, const char *t_file, const char *t_functionName, int t_line,
                     int t_mpiStatus);

        [[nodiscard]] const char * what () const noexcept final;

        std::string_view getInfo();
        std::string_view getFile();
        int getLine();
        int getMpiStatus();

    private:
        std::string m_info;
        std::string m_file;
        int m_line;
        std::string m_functionName;
        int m_mpiStatus;
    };
}

#endif //SOURCES_MPIEXCEPTION_HPP
