//
// Created by denis on 19.03.23.
//

#include "include/logging.h"

std::string getLoggingFilename(int rank)
{
    return "Process_" + std::to_string(rank) + ".log";
}