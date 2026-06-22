# file_server
add_executable(file_server
    file_server.cpp
    ${PROTO_COMMON_SOURCES}
)
set_target_properties(file_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${OUTPUT_DIR})
target_link_libraries(file_server ${COMMON_LIBS})
