######### get arch ###########################################
execute_process(COMMAND uname -m
        OUTPUT_VARIABLE ARCH
        OUTPUT_STRIP_TRAILING_WHITESPACE
        COMMAND_ERROR_IS_FATAL ANY
)
set(OS_ARCH ${ARCH})
message(STATUS "os arch ${OS_ARCH}") # x86_64 or aarch64

# download
execute_process(
        COMMAND bash deps/dep_create.sh ${COMMUNITY_BUILD} tool ${DEP_VAR} obdevtools-gcc9
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMAND_ERROR_IS_FATAL ANY)
SET(COMPILER_DIR ${DEP_VAR}/usr/local/oceanbase/devtools/bin/)
SET(GCC_LIB_DIR ${DEP_VAR}/usr/local/oceanbase/devtools/lib/gcc/x86_64-redhat-linux/9)

message(STATUS "COMPILER_DIR: ${COMPILER_DIR}")

find_program(CC NAMES gcc PATHS ${COMPILER_DIR} NO_DEFAULT_PATH)
find_program(CXX NAMES g++ PATHS ${COMPILER_DIR} NO_DEFAULT_PATH)
find_program(AR NAMES gcc-ar ar PATHS ${COMPILER_DIR} NO_DEFAULT_PATH)
find_program(RANLIB NAMES gcc-ranlib ranlib PATHS ${COMPILER_DIR} NO_DEFAULT_PATH)
set(CMAKE_C_COMPILER ${CC})
set(CMAKE_CXX_COMPILER ${CXX})
set(CMAKE_AR ${AR})
set(CMAKE_RANLIB ${RANLIB})
#CCACHE INCREASES COMPILATION SPEED
find_program(CCACHE_PROGRAM ccache)
if (CCACHE_PROGRAM)
    message(STATUS "CCACHE_PROGRAM:${CCACHE_PROGRAM}")
    set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
    set(CMAKE_CUDA_COMPILER_LAUNCHER "${CCACHE_PROGRAM}") # CMake 3.9+
endif ()
message(STATUS "C compiler: ${CMAKE_C_COMPILER}")
message(STATUS "C++ compiler: ${CMAKE_CXX_COMPILER}")
message(STATUS "ar: ${CMAKE_AR}")
message(STATUS "ranlib: ${CMAKE_RANLIB}")

GET_FILENAME_COMPONENT(COMPILER_DIR ${CMAKE_C_COMPILER} DIRECTORY)
GET_FILENAME_COMPONENT(COMPILER_BASE_DIR ${COMPILER_DIR} DIRECTORY)
SET(CXX_LIB_DIR ${COMPILER_BASE_DIR}/lib64/)
message(STATUS "CXX_LIB_DIR: ${CXX_LIB_DIR}, GCC_LIB_DIR: ${GCC_LIB_DIR}")

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Choose the type of build, options are: Debug Release RelWithDebInfo MinSizeRel" FORCE)
endif ()
message(STATUS "build type: ${CMAKE_BUILD_TYPE}")

set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS 0)
set(CMAKE_POSITION_INDEPENDENT_CODE 1)

## ensure that the build results can be run on systems with lower libstdc++ version than the build system
add_compile_definitions($<$<COMPILE_LANGUAGE:CXX>:_GLIBCXX_USE_CXX11_ABI=0>)
link_directories(${CXX_LIB_DIR} ${GCC_LIB_DIR})
add_link_options($<$<COMPILE_LANGUAGE:CXX>:-static-libstdc++>)
add_link_options($<$<COMPILE_LANGUAGE:CXX,C>:-static-libgcc>)

if (WITH_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif ()

if (WITH_DEBUG)
    add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-ggdb>)
else ()
    add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-ggdb>)
    add_compile_definitions($<$<COMPILE_LANGUAGE:CXX>:NDEBUG>)
endif ()

add_compile_definitions($<$<COMPILE_LANGUAGE:C>:__STDC_LIMIT_MACROS>)
add_compile_options($<$<COMPILE_LANGUAGE:CXX,C>:-Wall>)
add_compile_options($<$<COMPILE_LANGUAGE:CXX,C>:-Werror>)
## Organize the three-party compilation, and then remove the ignored item
add_compile_options($<$<COMPILE_LANGUAGE:CXX,C>:-Wno-sign-compare>)
add_compile_options($<$<COMPILE_LANGUAGE:CXX,C>:-Wno-class-memaccess>)
add_compile_options($<$<COMPILE_LANGUAGE:CXX,C>:-Wno-reorder>)
if (OS_ARCH STREQUAL "x86_64")
    add_compile_options($<$<COMPILE_LANGUAGE:CXX,C>:-m64>)
endif ()
add_compile_options($<$<COMPILE_LANGUAGE:CXX,C>:-pipe>)
add_compile_options($<$<COMPILE_LANGUAGE:CXX,C>:-fPIC>)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -pipe -pie -znoexecstack -znow")
#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}-fPIC -pipe -pie -znoexecstack -znow")
#add_link_options(-fPIC -pipe -pie -znoexecstack -znow)

add_compile_options($<$<COMPILE_LANGUAGE:CXX,C>:-pie>)
add_compile_options($<$<COMPILE_LANGUAGE:CXX,C>:-znoexecstack>)
add_compile_options($<$<COMPILE_LANGUAGE:CXX,C>:-znow>)
add_link_options(-lm)
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_link_options(-lrt)
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    add_link_options($<$<COMPILE_LANGUAGE:CXX,C>:
            "-framework CoreFoundation"
            "-framework CoreGraphics"
            "-framework CoreData"
            "-framework CoreText"
            "-framework Security"
            "-framework Foundation"
            "-Wl,-U,_MallocExtension_ReleaseFreeMemory"
            "-Wl,-U,_ProfilerStart"
            "-Wl,-U,_ProfilerStop")
endif ()

include(ProcessorCount)
ProcessorCount(NUM_OF_PROCESSOR)
message(STATUS "NUM_OF_PROCESSOR: ${NUM_OF_PROCESSOR}")
message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
