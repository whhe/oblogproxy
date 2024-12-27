include(ExternalProject)
set(JEMALLOC_INSTALL_DIR ${THIRD_PARTY_PATH}/install/jemalloc)
SET(JEMALLOC_SOURCES_DIR ${THIRD_PARTY_PATH}/jemalloc)
SET(JEMALLOC_LIBRARIES "${JEMALLOC_INSTALL_DIR}/lib/libjemalloc_pic.a" CACHE FILEPATH "jemalloc library." FORCE)
set(JEMALLOC_INCLUDE_DIR "${JEMALLOC_INSTALL_DIR}/include")
if (NOT EXISTS ${JEMALLOC_INCLUDE_DIR}/include)
    execute_process(COMMAND mkdir -p ${JEMALLOC_INCLUDE_DIR}/include COMMAND_ERROR_IS_FATAL ANY)
endif ()
ExternalProject_Add(extern_jemalloc
        PREFIX ${THIRD_PARTY_PATH}/jemalloc
        GIT_REPOSITORY "https://github.com/jemalloc/jemalloc.git"
        GIT_TAG "5.3.0"
        UPDATE_COMMAND ""
        CONFIGURE_COMMAND
        COMMAND ./autogen.sh
        COMMAND ./configure --disable-initial-exec-tls
        BUILD_COMMAND make
        BUILD_IN_SOURCE 1
        INSTALL_COMMAND mkdir -p ${JEMALLOC_INSTALL_DIR}/lib COMMAND cp -r include ${JEMALLOC_INSTALL_DIR}/ COMMAND cp lib/libjemalloc_pic.a ${JEMALLOC_LIBRARIES}
        CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${JEMALLOC_INSTALL_DIR}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE}
        -DCMAKE_CXX_FLAGS_DEBUG=${CMAKE_CXX_FLAGS_DEBUG}
        -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
)

add_library(jemalloc STATIC IMPORTED GLOBAL)
add_dependencies(jemalloc extern_jemalloc)
set_target_properties(jemalloc PROPERTIES IMPORTED_LOCATION ${JEMALLOC_LIBRARIES})
target_include_directories(jemalloc INTERFACE ${JEMALLOC_INCLUDE_DIR})