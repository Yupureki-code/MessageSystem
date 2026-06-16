# AGENTS.md - MessageSystem

## Build

```bash
cmake -B build && cmake --build build
```

C++17, CMake 3.28+. Build artifacts go in `build/` (gitignored).

## Services

Three independent RPC servers (brpc):

- **file_server** (`src/service/file/file_server.cpp`) - port 8001
- **user_server** (`src/service/user/user_server.cpp`) - port 8002
- **message_store_server** (`src/service/message_store/message_store_server.hpp`) - no main yet

Service discovery via etcd at `http://127.0.0.1:2379`.

## Key Dependencies

- brpc (RPC framework), protobuf, gflags
- ODB (MySQL ORM) - generated files in `src/comm_include/odb/`
- Redis, Elasticsearch (elasticlient)
- spdlog (logging), fmt

## Generated Code - DO NOT EDIT

- `src/proto/*.proto` → `src/comm_include/proto_include/*.pb.{h,cc}`
- `src/comm_include/odb/user/user-odb.{cxx,hxx,ixx}` - ODB generated
- `src/comm_include/odb/message/message-odb.{cxx,hxx,ixx}` - ODB generated
- `src/service/comm.pb.cc` - committed generated code

Regenerate protobuf: `protoc --cpp_out=. *.proto` from `src/proto/`

## Architecture

- `src/comm_include/` - shared headers (config, logging, Redis, ES, etcd, channels)
- `src/proto/` - protobuf definitions
- `src/service/` - service implementations

User server depends on File server (for avatars) - discovered via etcd.

## Config

`config.in.h` → `src/comm_include/config.h` via CMake `configure_file()`. SMTP config via env vars (`OJ_SMTP_*`).

## Testing

No formal test framework. `test/test.cpp` is a standalone jsoncpp test. Compile manually:
```bash
cd test && make
```

## Gotchas

- ODB files and protobuf `.pb.cc/.pb.h` are committed - don't regenerate unless protos change
- `src/comm_include/odb/friend/` exists but is empty (friend service not implemented)
- `message_store_server.hpp` has a syntax error in `MsgSearch` (missing closing brace at line 200)
- Chinese comments throughout codebase
