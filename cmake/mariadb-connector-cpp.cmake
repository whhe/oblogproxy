include(ExternalProject)

set(CONNCPP_INSTALL_DIR ${THIRD_PARTY_PATH}/install/mariadb-connector-cpp)

if (NOT EXISTS ${CONNCPP_INSTALL_DIR}/include/mariadb)
    execute_process(COMMAND mkdir -p ${CONNCPP_INSTALL_DIR}/include/mariadb COMMAND_ERROR_IS_FATAL ANY)
endif ()

add_library(libmariadbcpp SHARED IMPORTED GLOBAL)
if (NOT WITH_DEPS)
    ExternalProject_Add(
            extern_mariadb-connector-cpp
            ${EXTERNAL_PROJECT_LOG_ARGS}
            GIT_REPOSITORY "https://github.com/mariadb-corporation/mariadb-connector-cpp.git"
            GIT_TAG "1.1.3"
            PREFIX ${THIRD_PARTY_PATH}/mariadb-connector-cpp
            UPDATE_COMMAND ""
            CMAKE_ARGS
            -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
            -DCMAKE_INSTALL_PREFIX=${CONNCPP_INSTALL_DIR}
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON
            -DCONC_WITH_UNIT_TESTS=OFF
            -DWITH_SSL=OPENSSL
            -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
    )
    add_dependencies(libmariadbcpp extern_mariadb-connector-cpp)
endif ()

message("Header file search paths: ${CONNCPP_INSTALL_DIR}/include/mariadb")
set_target_properties(libmariadbcpp PROPERTIES
        IMPORTED_LOCATION ${CONNCPP_INSTALL_DIR}/lib64/mariadb/libmariadbcpp.so
        IMPORTED_SONAME libmariadb.so.3)
target_include_directories(libmariadbcpp INTERFACE ${CONNCPP_INSTALL_DIR}/include/mariadb)