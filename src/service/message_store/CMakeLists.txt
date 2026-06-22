# message_store_server
add_executable(message_store_server
    message_store_server.cpp
    ${PROTO_COMMON_SOURCES}
    ${ODB_MESSAGE_SOURCES}
)
set_target_properties(message_store_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${OUTPUT_DIR})
target_link_libraries(message_store_server ${COMMON_LIBS} ${DB_LIBS} ${ES_LIBS})
