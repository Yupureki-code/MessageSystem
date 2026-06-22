# user_server
add_executable(user_server
    user_server.cpp
    ${PROTO_COMMON_SOURCES}
    ${ODB_USER_SOURCES}
)
set_target_properties(user_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${OUTPUT_DIR})
target_link_libraries(user_server ${COMMON_LIBS} ${DB_LIBS} ${ES_LIBS} ${REDIS_LIBS})
