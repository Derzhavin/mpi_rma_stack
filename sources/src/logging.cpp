//
// Created by denis on 19.03.23.
//

#include <sstream>

#include "include/logging.h"

std::string getLoggingFilename(int rank, std::string_view info)
{
    std::ostringstream oss;
    oss << "Rank_"
        << rank
        << '_'
        << info
        << ".log";
    return oss.str();
}