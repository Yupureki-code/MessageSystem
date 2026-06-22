# task_transfer_server
add_executable(task_transfer_server
    task_transfer.cpp
    ${PROTO_COMMON_SOURCES}
    ${MQ_SOURCES}
)
set_target_properties(task_transfer_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${OUTPUT_DIR})
target_link_libraries(task_transfer_server ${COMMON_LIBS} ${MQ_LIBS} ${ES_LIBS})
