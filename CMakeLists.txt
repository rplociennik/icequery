cmake_minimum_required( VERSION 2.8.5 )

set( CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules )

find_package( Icecream REQUIRED )
find_package( ICU 4.6 REQUIRED )

if( CMAKE_COMPILER_IS_GNUCXX )
    set( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -Wextra" )
endif()

set( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DU_USING_ICU_NAMESPACE=0" )

include_directories(
    ${LIBICECREAM_INCLUDE_DIR}
    ${ICU_INCLUDE_DIRS}
)

add_executable( icequery icequery.cpp )

if( NOT ICU_I18N_FOUND )
    message( FATAL_ERROR "Could not find ICU_I18N" )
else()
    message( STATUS "ICU i18n ${ICU_VERSION} found")
endif()

target_link_libraries(
    icequery
    ${LIBICECREAM_LIBRARIES}
    ${ICU_LIBRARIES}
    ${ICU_I18N_LIBRARIES}
    ${CMAKE_DL_LIBS}
)
