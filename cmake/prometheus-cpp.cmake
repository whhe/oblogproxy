include(ExternalProject)
set(PROMETHEUS_INSTALL_DIR ${THIRD_PARTY_PATH}/install/prometheus-cpp)

if (NOT EXISTS ${PROMETHEUS_INSTALL_DIR}/include)
    execute_process(COMMAND mkdir -p ${PROMETHEUS_INSTALL_DIR}/include COMMAND_ERROR_IS_FATAL ANY)
endif ()
add_library(prometheus-cpp INTERFACE)
if (NOT WITH_DEPS)
    ExternalProject_Add(
            extern_prometheus-cpp
            ${EXTERNAL_PROJECT_LOG_ARGS}
            GIT_REPOSITORY "https://github.com/jupp0r/prometheus-cpp.git"
            GIT_TAG "v1.1.0"
            PREFIX ${THIRD_PARTY_PATH}/prometheus-cpp
            CMAKE_ARGS
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON
            -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
            -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
            -DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE}
            -DCMAKE_CXX_FLAGS_DEBUG=${CMAKE_CXX_FLAGS_DEBUG}
            -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
            -DCMAKE_INSTALL_PREFIX=${PROMETHEUS_INSTALL_DIR}
            -DENABLE_PUSH=ON
            -DENABLE_PULL=ON
            -DENABLE_TESTING=OFF
            -DENABLE_COMPRESSION=ON
            -DUSE_THIRDPARTY_LIBRARIES=ON
    )
    add_dependencies(prometheus-cpp extern_prometheus-cpp)
endif ()
target_link_libraries(prometheus-cpp INTERFACE
        ${PROMETHEUS_INSTALL_DIR}/lib64/libprometheus-cpp-pull.a
        ${PROMETHEUS_INSTALL_DIR}/lib64/libprometheus-cpp-push.a
        ${PROMETHEUS_INSTALL_DIR}/lib64/libprometheus-cpp-core.a
)
target_include_directories(prometheus-cpp INTERFACE ${PROMETHEUS_INSTALL_DIR}/include)