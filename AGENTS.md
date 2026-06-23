# AGENTS.md - MessageSystem

## Build

```bash
cmake -B build && cmake --build build -j$(nproc)
```

C++17, CMake 3.28+. Binaries output to `output/`. Tests in `test/gtest/build/`.

## Services (8 total, brpc)

| Service | Port | Source |
|---------|------|--------|
| file_server | 8001 | `src/service/file/` |
| user_server | 8002 | `src/service/user/` |
| conversation_server | 8003 | `src/service/conversation/` |
| message_server | 8004 | `src/service/message/` |
| message_store_server | 8005 | `src/service/message_store/` |
| friend_server | 8006 | `src/service/friend/` |
| task_transfer_server | 8007 | `src/service/task_transfer/` |
| gateway_server | 8000(BRPC) / 9000(WS) | `src/service/gateway/` |

Service discovery via etcd at `http://127.0.0.1:2379`.

## Key Dependencies

- brpc (RPC), protobuf, gflags
- ODB (MySQL ORM) — `src/comm_include/odb/`
- Redis (sw::redis++), Elasticsearch (elasticlient), RabbitMQ (AMQP-CPP)
- spdlog (logging), fmt, jsoncpp, boost (beast/asio)
- etcd-cpp-api, cpprest, libcurl

## Generated Code — DO NOT EDIT

- `src/proto/*.proto` → regenerate: `cd src/proto && protoc --cpp_out=. *.proto && cp *.pb.h *.pb.cc ../comm_include/proto_include/`
- ODB: regenerate with `odb -d mysql --std c++11 --generate-query --generate-schema foo.hxx`

## Architecture

```
gateway (BRPC:8000 / WS:9000) ──→ user(8002), friend(8006), conversation(8003), message(8004)
message_server(8004) ──→ RabbitMQ ──→ task_transfer(8007) ──→ message_store(8005)
```

- `src/comm_include/` — shared: channel.hpp, redis.hpp, es.hpp, etcd.hpp, rabbitmq.hpp, messageDB.hpp
- `src/service/` — one subdir per service, each with `*_server.hpp` + `.cpp` + `CMakeLists.txt`

## Critical Gotchas

### Service entry points
- **Every service `main()` MUST call `initLogger()` first**, otherwise `g_logger` is null → segfault on any `LOG_*`
- **Every service `main()` MUST block with `while(true) sleep(60)`** after `Start()` — otherwise the service exits immediately
- **Server Wrapper `Start()`**: `brpc::Server` must be a class member (`unique_ptr`), NOT a local variable. If local, it destructs on return → "going to quit"

### BRPC RPC overrides
- **Request parameters MUST be `const`** to match protobuf-generated base class signatures. Without `const`, methods silently fail to override → "Method not implemented" at runtime.
- Compile gateway with `-fpermissive` to allow `const` protobuf mutations

### libcurl
- System `/usr/lib/` has SMTP support; `/usr/local/lib/` (7.69.1) does NOT.
- `libcpr.so` and `libelasticlient.so` link against local libcurl → SMTP fails.
- **Fix**: RPATH must list system paths first: `set(CMAKE_BUILD_RPATH "/lib/x86_64-linux-gnu:/usr/lib/x86_64-linux-gnu")`

### ES 8.x
- Requires HTTPS + authentication. Use env: `IM_ES_HOST=https://127.0.0.1:9200`, `IM_ES_USER=`, `IM_ES_PASS=`
- `ESInsert`, `ESQuery` etc. default to global `es_client` (null). Always pass explicit client: `ESInsert es(client)` or check `if (!_client) return;`

### ODB
- Message: `#pragma db id` (NOT `id auto`) — message_id is manually assigned
- `ConversationMember.id` must be manually set (no auto-increment). Use timestamp+index for uniqueness.
- ODB `persist()` requires non-const reference

### WS Server
- `WsServer::run()` MUST be called explicitly after construction — the constructor alone does not start listening

### ServiceManager
- `chooseService()` returns `Response` — **always check `rep.status`** before using the channel, otherwise null channel → timeout

## Environment Variables

| Var | Default | Purpose |
|-----|---------|---------|
| `IM_MYSQL_HOST/USER/PASSWD/DB` | - | MySQL connection |
| `IM_REDIS_HOST/PORT` | 127.0.0.1:6379 | Redis connection |
| `IM_ES_HOST/USER/PASS` | - | Elasticsearch |
| `IM_MQ_USER/PASSWD/HOST` | guest/guest | RabbitMQ |
| `IM_SMTP_HOST/PORT/USER/PASS/FROM/SSL` | - | SMTP mail |

## Testing

All tests in `test/gtest/`. Build and run:

```bash
cd test/gtest/build && cmake .. && make -j4

# Unit tests (no services needed):
MYSQL_HOST=127.0.0.1 MYSQL_USER=test MYSQL_PASSWORD=test123 MYSQL_DB=test ./test_database
MYSQL_HOST=127.0.0.1 MYSQL_USER=test MYSQL_PASSWORD=test123 MYSQL_DB=test ./test_friend
./test_task_transfer

# BRPC tests (services must be running):
./test_file           # needs file_server:8001
./test_user           # needs user_server:8002
./test_conversation   # needs conversation_server:8003
./test_message_store 127.0.0.1:8005  # needs message_store:8005
./test_gateway_brpc   # needs gateway:8000
./test_gateway_ws     # needs gateway WS:9000
```

Services must be started with all IM_* env vars exported, then use `nohup output/X --listen_port=Y & disown`.

## spdlog Format

```cpp
LOG_DEBUG("key: {}", value);    // ✅ correct
LOG_DEBUG("key:", value);       // ❌ segfaults
```
