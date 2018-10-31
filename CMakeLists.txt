cmake_minimum_required(VERSION 3.12)
project(final_project C)
# The version number.
set (Tutorial_VERSION_MAJOR 1)
set (Tutorial_VERSION_MINOR 0)


include_directories(src/include /src)

set(CMAKE_C_STANDARD 99)

set(SOURCE_FILES
        src/mydb_manager.c
        src/include/mydb_manager.h
        src/include/common.h
        src/include/message.h
        src/include/utils.h
        src/utils.c
        src/include/parse.h
        src/parse.c
        src/include/client_context.h
        src/client_context.c
        src/include/client_context.h
        )



file(GLOB SOURCES
        *.h
        *.c
        )


add_executable(client ${SOURCE_FILES} src/client.c)



add_executable(server ${SOURCE_FILES} src/server.c)



add_executable(test ${SOURCE_FILES} src/main.c)
