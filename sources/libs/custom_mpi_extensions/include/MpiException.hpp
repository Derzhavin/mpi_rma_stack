//
// Created by denis on 02.02.23.
//

#ifndef SOURCES_MPIEXCEPTION_HPP
#define SOURCES_MPIEXCEPTION_HPP


#include <string>
#include <iostream>
#include <exception>
#include <string_view>

namespace custom_mpi_extensions
{
    class MpiException: public std::exception
    {
    public:
        MpiException(const char *t_info, const char *t_file, const char *t_functionName, int t_line,
                     int t_mpiStatus);

        [[nodiscard]] const char * what () const noexcept final;

        std::string_view getInfo() const;
        std::string_view getFile() const;
        int getLine() const;
        int getMpiStatus() const;

    private:
        std::string m_info;
        std::string m_file;
        int m_line;
        std::string m_functionName;
        int m_mpiStatus;

    };
}

std::ostream& operator<<(std::ostream& out, const custom_mpi_extensions::MpiException& ex);
#endif //SOURCES_MPIEXCEPTION_HPP
