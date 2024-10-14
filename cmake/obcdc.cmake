# header-only target
#add_library(logmsg_wrapper INTERFACE)
#target_include_directories(logmsg_wrapper INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src/obcdcaccess/logmsg)

message(STATUS "community build: ${COMMUNITY_BUILD}")

# install tools
execute_process(
        COMMAND bash deps/dep_create.sh ${COMMUNITY_BUILD} tool ${DEP_VAR}
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        COMMAND_ERROR_IS_FATAL ANY)
add_library(libaio SHARED IMPORTED GLOBAL)
set_target_properties(libaio PROPERTIES
        IMPORTED_LOCATION ${DEP_VAR}/usr/local/oceanbase/deps/devel/lib/libaio.so.1.0.1
        IMPORTED_SONAME libaio.so.1)
target_include_directories(libaio INTERFACE ${DEP_VAR}/usr/local/oceanbase/deps/devel/include)

if (NOT COMMUNITY_BUILD)
    # install liboblog2
    set(OB_DEVEL_2_BASE_DIR ${DEP_VAR}/oceanbase-devel-2)
    execute_process(
            COMMAND bash deps/dep_create.sh ${COMMUNITY_BUILD} cdc ${DEP_VAR} oceanbase-devel 2 ${OB_DEVEL_2_BASE_DIR}
            WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
            COMMAND_ERROR_IS_FATAL ANY)

    add_library(liboblog2 SHARED IMPORTED GLOBAL)
    set_target_properties(liboblog2 PROPERTIES
            IMPORTED_LOCATION ${OB_DEVEL_2_BASE_DIR}/home/admin/oceanbase/lib/liboblog.so.1.0.0
            IMPORTED_SONAME liboblog.so.1)
    target_include_directories(liboblog2 INTERFACE ${OB_DEVEL_2_BASE_DIR}/home/admin/oceanbase/include)
    target_compile_definitions(liboblog2 INTERFACE _OBCDC_V2_)
    target_link_libraries(liboblog2 INTERFACE libaio)

    # install liboblog3
    set(OB_DEVEL_3_BASE_DIR ${DEP_VAR}/oceanbase-devel-3)
    execute_process(
            COMMAND bash deps/dep_create.sh ${COMMUNITY_BUILD} cdc ${DEP_VAR} oceanbase-devel 3 ${OB_DEVEL_3_BASE_DIR}
            WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
            COMMAND_ERROR_IS_FATAL ANY)

    add_library(liboblog3 SHARED IMPORTED GLOBAL)
    set_target_properties(liboblog3 PROPERTIES
            IMPORTED_LOCATION ${OB_DEVEL_3_BASE_DIR}/home/admin/oceanbase/lib/liboblog.so.1.0.0
            IMPORTED_SONAME liboblog.so.1)
    target_include_directories(liboblog3 INTERFACE ${OB_DEVEL_3_BASE_DIR}/home/admin/oceanbase/include
            INTERFACE ${OB_DEVEL_3_BASE_DIR}/home/admin/oceanbase/include/drcmsg)
    target_link_libraries(liboblog3 INTERFACE libaio)


    execute_process(
            COMMAND bash deps/find_dep_config_file.sh
            OUTPUT_VARIABLE OUTPUT
            OUTPUT_STRIP_TRAILING_WHITESPACE
            COMMAND_ERROR_IS_FATAL ANY
            WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    )
    message("OUTPUT_VARIABLE:${OUTPUT}")
    file(STRINGS ${OUTPUT} file_contents)
    set(version_list)

    foreach (line IN LISTS file_contents)
        if (NOT line)
            continue()
        endif ()
        string(REGEX MATCH "oceanbase-cdc-([0-9]+\\.[0-9]+\\.[0-9]+)" version_match ${line})
        if (version_match)
            list(APPEND version_list ${CMAKE_MATCH_1})
        endif ()
    endforeach ()

    list(REMOVE_DUPLICATES version_list)

    message("Unique versions:")
    foreach (version IN LISTS version_list)
        message(${version})
        # install libobcdc421
        set(OB_CDC_4_BASE_DIR ${DEP_VAR}/oceanbase-cdc-${version})
        execute_process(
                COMMAND bash deps/dep_create.sh ${COMMUNITY_BUILD} cdc ${DEP_VAR} oceanbase-cdc ${version} ${OB_CDC_4_BASE_DIR}
                WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
                COMMAND_ERROR_IS_FATAL ANY)

        add_library(libobcdc${version} SHARED IMPORTED GLOBAL)
        file(REAL_PATH ${OB_CDC_4_BASE_DIR}/home/admin/oceanbase/lib64/libobcdc.so.4 LIBOBCDC_SO EXPAND_TILDE)
        set_target_properties(libobcdc${version} PROPERTIES
                IMPORTED_LOCATION ${LIBOBCDC_SO}
                IMPORTED_SONAME libobcdc.so.4)
        target_include_directories(libobcdc${version} INTERFACE ${OB_CDC_4_BASE_DIR}/home/admin/oceanbase/include/libobcdc
                INTERFACE ${OB_CDC_4_BASE_DIR}/home/admin/oceanbase/include/drcmsg
                INTERFACE ${OB_CDC_4_BASE_DIR}/home/admin/oceanbase/include)
        target_compile_definitions(libobcdc${version}
                INTERFACE _OBCDC_H_
                INTERFACE _OBCDC_NS_
                INTERFACE OB_USE_DRCMSG)
        target_link_libraries(libobcdc${version} INTERFACE libaio)
    endforeach ()
else ()
    add_library(libmariadb SHARED IMPORTED GLOBAL)
    set_target_properties(libmariadb PROPERTIES
            IMPORTED_LOCATION ${DEP_VAR}/usr/local/oceanbase/deps/devel/lib/mariadb/libmariadb.so.3
            IMPORTED_SONAME libmariadb.so)
    target_include_directories(libmariadb INTERFACE ${DEP_VAR}/usr/local/oceanbase/deps/devel/include/mariadb)

    # install libobcdcce3
    set(OB_CE_DEVEL_3_BASE_DIR ${DEP_VAR}/oceanbase-ce-devel-3)
    execute_process(
            COMMAND bash deps/dep_create.sh ${COMMUNITY_BUILD} cdc ${DEP_VAR} oceanbase-ce-devel 3 ${OB_CE_DEVEL_3_BASE_DIR}
            WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
            COMMAND_ERROR_IS_FATAL ANY)

    add_library(libobcdcce3 SHARED IMPORTED GLOBAL)
    set_target_properties(libobcdcce3 PROPERTIES
            IMPORTED_LOCATION ${OB_CE_DEVEL_3_BASE_DIR}/usr/lib/libobcdc.so.1.0.0
            IMPORTED_SONAME libobcdc.so.1)
    target_include_directories(libobcdcce3 INTERFACE ${OB_CE_DEVEL_3_BASE_DIR}/usr/include)
    target_compile_definitions(libobcdcce3
            INTERFACE _OBCDC_H_
            INTERFACE _OBLOG_MSG_)
    target_link_libraries(libobcdcce3 INTERFACE libaio)

    execute_process(
            COMMAND bash deps/find_dep_config_file.sh
            OUTPUT_VARIABLE OUTPUT
            OUTPUT_STRIP_TRAILING_WHITESPACE
            COMMAND_ERROR_IS_FATAL ANY
            WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    )
    message("OUTPUT_VARIABLE:${OUTPUT}")
    file(STRINGS ${OUTPUT} file_contents)
    set(version_list)

    foreach (line IN LISTS file_contents)
        if (NOT line)
            continue()
        endif ()
        string(REGEX MATCH "oceanbase-ce-cdc-([0-9]+\\.[0-9]+\\.[0-9]+)" version_match ${line})
        if (version_match)
            list(APPEND version_list ${CMAKE_MATCH_1})
        endif ()
    endforeach ()

    list(REMOVE_DUPLICATES version_list)

    message("version_list is: ${version_list}")

    message("Unique versions:")
    foreach (version IN LISTS version_list)
        message(${version})
        # install libobcdc421
        set(OB_CE_CDC_4_BASE_DIR ${DEP_VAR}/oceanbase-ce-cdc-${version})
        execute_process(
                COMMAND bash deps/dep_create.sh ${COMMUNITY_BUILD} cdc ${DEP_VAR} oceanbase-ce-cdc ${version} ${OB_CE_CDC_4_BASE_DIR}
                WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
                COMMAND_ERROR_IS_FATAL ANY)

        add_library(libobcdcce${version} SHARED IMPORTED GLOBAL)
        file(REAL_PATH ${OB_CE_CDC_4_BASE_DIR}/home/admin/oceanbase/lib64/libobcdc.so.4 LIBOBCDC_SO EXPAND_TILDE)
        set_target_properties(libobcdcce${version} PROPERTIES
                IMPORTED_LOCATION ${LIBOBCDC_SO}
                IMPORTED_SONAME libobcdc.so.4)
        target_include_directories(libobcdcce${version} INTERFACE ${OB_CE_CDC_4_BASE_DIR}/home/admin/oceanbase/include/libobcdc
                INTERFACE ${OB_CE_CDC_4_BASE_DIR}/home/admin/oceanbase/include/oblogmsg
                INTERFACE ${OB_CE_CDC_4_BASE_DIR}/home/admin/oceanbase/include)
        target_compile_definitions(libobcdcce${version}
                INTERFACE _OBCDC_H_
                INTERFACE _OBCDC_NS_
                INTERFACE _OBLOG_MSG_)
        target_link_libraries(libobcdcce${version}
                INTERFACE libaio
                INTERFACE libmariadb)
    endforeach ()
endif ()

get_target_property(LIBAIO_INCLUDE_PATH libaio INTERFACE_INCLUDE_DIRECTORIES)
get_target_property(LIBAIO_LIBRARIES libaio IMPORTED_LOCATION)
message(STATUS "libaio: ${LIBAIO_INCLUDE_PATH}, ${LIBAIO_LIBRARIES}")

#get_target_property(LOGMSG_WRAPPER_INCLUDE_PATH logmsg_wrapper INTERFACE_INCLUDE_DIRECTORIES)
#get_target_property(LOGMSG_WRAPPER_DEPS logmsg_wrapper INTERFACE_LINK_LIBRARIES)
#message(STATUS "logmsg_wrapper: ${LOGMSG_WRAPPER_INCLUDE_PATH}, deps: ${LOGMSG_WRAPPER_DEPS}")

###################### deps
CMAKE_POLICY(SET CMP0074 NEW)