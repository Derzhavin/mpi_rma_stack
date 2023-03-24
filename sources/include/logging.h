//
// Created by denis on 19.03.23.
//

#ifndef SOURCES_LOGGING_H
#define SOURCES_LOGGING_H

#include <string>

std::string getLoggingFilename(int rank);

constexpr inline std::string_view mainLoggerName{"MainLogger"};
#endif //SOURCES_LOGGING_H
