include(ExternalProject)

set(GDAL_INSTALL_DIR ${THIRD_PARTY_PATH}/install/gdal)

if (NOT EXISTS ${GDAL_INSTALL_DIR}/include)
    execute_process(COMMAND mkdir -p ${GDAL_INSTALL_DIR}/include COMMAND_ERROR_IS_FATAL ANY)
endif ()

add_library(gdal SHARED IMPORTED GLOBAL)
if (NOT WITH_DEPS)
    ExternalProject_Add(
            extern_gdal
            DEPENDS proj
            ${EXTERNAL_PROJECT_LOG_ARGS}
            GIT_REPOSITORY "https://github.com/OSGeo/gdal.git"
            GIT_TAG "v3.7.3"
            PREFIX ${THIRD_PARTY_PATH}/gdal
            UPDATE_COMMAND ""
            CMAKE_ARGS
            -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
            -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
            -DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE}
            -DCMAKE_CXX_FLAGS_DEBUG=${CMAKE_CXX_FLAGS_DEBUG}
            -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
            -DCMAKE_INSTALL_PREFIX=${GDAL_INSTALL_DIR}
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON
            -DBUILD_TESTING=OFF
            -DBUILD_PYTHON_BINDINGS=OFF
            -DBUILD_JAVA_BINDINGS=OFF
            -DBUILD_CSHARP_BINDINGS=OFF
            -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
            -DPROJ_INCLUDE_DIR=${THIRD_PARTY_PATH}/install/proj/include
            -DPROJ_LIBRARY_RELEASE=${THIRD_PARTY_PATH}/install/proj/lib64/libproj.a
    )
    add_dependencies(gdal extern_gdal)
endif ()

set_target_properties(gdal PROPERTIES
        IMPORTED_LOCATION ${GDAL_INSTALL_DIR}/lib64/libgdal.so.33
        IMPORTED_SONAME libgdal.so.33.3.7.3)
target_include_directories(gdal INTERFACE ${GDAL_INSTALL_DIR}/include)
