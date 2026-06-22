# friend_server
add_executable(friend_server
    friend_server.cpp
    ${PROTO_COMMON_SOURCES}
    ${ODB_USER_SOURCES}
    ${ODB_FRIEND_SOURCES}
)
set_target_properties(friend_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${OUTPUT_DIR})
target_link_libraries(friend_server ${COMMON_LIBS} ${DB_LIBS})
