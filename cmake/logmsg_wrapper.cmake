add_library(logmsg_wrapper INTERFACE)
target_include_directories(logmsg_wrapper INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src/logmsg)
if (NOT COMMUNITY_BUILD)
    # oblogmsg wrapper
    add_library(drcmsg SHARED IMPORTED GLOBAL)
    set_target_properties(drcmsg PROPERTIES IMPORTED_LOCATION ${DEP_VAR}/usr/local/oceanbase/deps/devel/lib/libdrcmsg.a)
    target_include_directories(drcmsg INTERFACE ${DEP_VAR}/usr/local/oceanbase/deps/devel/include/drcmsg)
    target_link_libraries(logmsg_wrapper INTERFACE drcmsg)

    get_target_property(DRCMSG_INCLUDE_PATH drcmsg INTERFACE_INCLUDE_DIRECTORIES)
    get_target_property(DRCMSG_LOCATION drcmsg IMPORTED_LOCATION)
    message(STATUS "DRCMSG_INCLUDE_PATH: ${DRCMSG_INCLUDE_PATH}, DRCMSG_LOCATION: ${DRCMSG_LOCATION}")
else ()
    # oblogmsg wrapper
    add_library(oblogmsg SHARED IMPORTED GLOBAL)
    set_target_properties(oblogmsg PROPERTIES IMPORTED_LOCATION ${DEP_VAR}/usr/local/oceanbase/deps/devel/lib/liboblogmsg.a)
    target_include_directories(oblogmsg INTERFACE ${DEP_VAR}/usr/local/oceanbase/deps/devel/include/oblogmsg)
    target_link_libraries(logmsg_wrapper INTERFACE oblogmsg)

    get_target_property(OBLOGMSG_INCLUDE_PATH oblogmsg INTERFACE_INCLUDE_DIRECTORIES)
    get_target_property(OBLOGMSG_LOCATION oblogmsg IMPORTED_LOCATION)
    message(STATUS "OBLOGMSG_INCLUDE_PATH: ${OBLOGMSG_INCLUDE_PATH}, OBLOGMSG_LOCATION: ${OBLOGMSG_LOCATION}")
endif ()