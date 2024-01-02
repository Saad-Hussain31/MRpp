include_directories(../include)
file(GLOB_RECURSE MRPP_SOURCES "*.cpp")

add_library(MRppLib ${MRPP_SOURCES})
