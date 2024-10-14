include(ExternalProject)

set(PROJ_INSTALL_DIR ${THIRD_PARTY_PATH}/install/proj)

if (NOT EXISTS ${PROJ_INSTALL_DIR}/include)
    execute_process(COMMAND mkdir -p ${PROJ_INSTALL_DIR}/include COMMAND_ERROR_IS_FATAL ANY)
endif ()

add_library(proj STATIC IMPORTED GLOBAL)
if (NOT WITH_DEPS)
    ExternalProject_Add(
            extern_proj
            ${EXTERNAL_PROJECT_LOG_ARGS}
            GIT_REPOSITORY "https://github.com/OSGeo/PROJ.git"
            GIT_TAG "9.3.1"
            PREFIX ${THIRD_PARTY_PATH}/proj
            UPDATE_COMMAND ""
            CMAKE_ARGS
            -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
            -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
            -DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE}
            -DCMAKE_CXX_FLAGS_DEBUG=${CMAKE_CXX_FLAGS_DEBUG}
            -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
            -DCMAKE_INSTALL_PREFIX=${PROJ_INSTALL_DIR}
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON
            -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
            -DENABLE_CURL=OFF
            -DENABLE_TIFF=OFF
            -DBUILD_TESTING=OFF
            -DBUILD_SHARED_LIBS=OFF
            -DBUILD_PROJSYNC=OFF
    )
    add_dependencies(proj extern_proj)
endif ()

set_target_properties(proj PROPERTIES IMPORTED_LOCATION ${PROJ_INSTALL_DIR}/lib64/libproj.a)
target_include_directories(proj INTERFACE ${PROJ_INSTALL_DIR}/include)
