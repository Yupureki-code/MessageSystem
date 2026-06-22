# message_server
add_executable(message_server
    message_server.cpp
    ${PROTO_COMMON_SOURCES}
    ${MQ_SOURCES}
)
set_target_properties(message_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${OUTPUT_DIR})
target_link_libraries(message_server ${COMMON_LIBS} ${REDIS_LIBS} ${MQ_LIBS})
