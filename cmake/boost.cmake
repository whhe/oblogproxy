add_library(boost INTERFACE)
set(BOOST_INSTALL_DIR ${DEP_VAR}/usr/local/oceanbase/deps/devel)
target_link_libraries(boost INTERFACE
        ${BOOST_INSTALL_DIR}/lib/libboost_system.a
        ${BOOST_INSTALL_DIR}/lib/libboost_thread.a
)
target_include_directories(boost INTERFACE ${BOOST_INSTALL_DIR}/include)