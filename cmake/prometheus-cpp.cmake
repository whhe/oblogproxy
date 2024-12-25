add_library(prometheus-cpp INTERFACE)
set(PROMETHEUS_INSTALL_DIR ${DEP_VAR}/usr/local/oceanbase/deps/devel)
target_link_libraries(prometheus-cpp INTERFACE
        ${PROMETHEUS_INSTALL_DIR}/lib64/libprometheus-cpp-pull.a
        ${PROMETHEUS_INSTALL_DIR}/lib64/libprometheus-cpp-push.a
        ${PROMETHEUS_INSTALL_DIR}/lib64/libprometheus-cpp-core.a
)
target_include_directories(prometheus-cpp INTERFACE ${PROMETHEUS_INSTALL_DIR}/include)