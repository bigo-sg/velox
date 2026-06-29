file(READ "CMakeLists.txt" _cppkafka_cmake)
string(REPLACE
       [=[
if(NOT TARGET uninstall)
    # Confiugure the uninstall script
    configure_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/cmake_uninstall.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
        IMMEDIATE @ONLY
    )

    # Add uninstall target
    add_custom_target(uninstall
        COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)
endif()
]=]
       ""
       _cppkafka_cmake
       "${_cppkafka_cmake}")
file(WRITE "CMakeLists.txt" "${_cppkafka_cmake}")
