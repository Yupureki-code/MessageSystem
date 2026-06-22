# gateway_server
add_executable(gateway_server
    gateway.cpp
    ${PROTO_COMMON_SOURCES}
)
set_target_properties(gateway_server PROPERTIES 
    RUNTIME_OUTPUT_DIRECTORY ${OUTPUT_DIR}
    COMPILE_FLAGS "-fcoroutines"
    CXX_STANDARD 20
)
target_link_libraries(gateway_server ${COMMON_LIBS} ${REDIS_LIBS} ${GATEWAY_LIBS})
