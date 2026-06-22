# conversation_server
add_executable(conversation_server
    conversation_server.cpp
    ${PROTO_COMMON_SOURCES}
    ${ODB_CONVERSATION_SOURCES}
    ${ODB_USER_SOURCES}
)
set_target_properties(conversation_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${OUTPUT_DIR})
target_link_libraries(conversation_server ${COMMON_LIBS} ${DB_LIBS} ${ES_LIBS})
