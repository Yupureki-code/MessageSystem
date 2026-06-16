# MessageSystem 开发计划

## 需求概述

### 1. 数据模型优化
- 将 `message.hxx` 中 `create_time` 字段从 `boost::posix_time::ptime` 改为 `unsigned long long` 毫秒级时间戳
- 将 `content` 字段重命名为 `text`
- 将 `comm.proto` 中 `MessageInfo` 的 `UserInfo sender` 改为 `string sender_id`

### 2. 功能完善
- 完善 `messageDB` 中的 `PostFileMessages` 函数
- 新增 `DeleteTextMessages` 和 `DeleteFileMessages` 接口
- 完善 `MessageServiceImpl` 中的 `PostMessages` 和 `DeleteMessages` 接口
- 文件服务新增批量删除文件接口

### 3. 事务处理机制
- 使用本地事务表 `message_outbox` 保证数据一致性
- 采用重试机制：最多5次，超过则回滚
- 文件删除采用延迟删除策略（24小时后删除）

---

## 技术决策

### 【决策主题】: 时间戳精度选择
【背景分析】: 当前使用 `boost::posix_time::ptime` 存储时间，需要引入 Boost 库
【备选方案】:
- 方案A: 使用 `unsigned long long` 毫秒时间戳 + 优点：无需 Boost 依赖，性能高；缺点：可读性差
- 方案B: 使用 `std::chrono::system_clock::time_point` + 优点：C++标准库；缺点：ODB支持有限
【最终选择】: 方案A
【决策理由】: 减少依赖，提高性能，与 ES 时间戳格式一致
【风险提示】: 需要修改所有涉及时间处理的代码

### 【决策主题】: 删除策略选择
【背景分析】: 删除消息时需要同时处理 MySQL、ES 和文件服务
【备选方案】:
- 方案A: 硬删除 + 优点：数据清理彻底；缺点：恢复困难
- 方案B: 软删除 + 优点：可恢复；缺点：数据冗余
【最终选择】: 方案A（硬删除）
【决策理由】: 用户明确要求，减少数据冗余
【风险提示】: 需要确保删除操作的原子性

### 【决策主题】: 并发控制策略
【背景分析】: 多用户同时发送消息和撤回消息的场景
【备选方案】:
- 方案A: 乐观锁 + 优点：性能好；缺点：冲突时重试
- 方案B: 分布式锁 + 优点：强一致；缺点：性能差
【最终选择】: 方案A（乐观锁）
【决策理由】: 消息发送无需防并发，撤回操作使用乐观锁
【风险提示】: 需要处理乐观锁冲突的重试逻辑

---

## 开发任务

### 任务 1: 数据模型修改
【任务ID】: TASK-001
【任务名称】: 修改 Message 数据结构
【优先级】: 高
【预估工时】: 2小时
【技术要点】:
- 修改 `message.hxx` 中 `create_time` 字段类型
- 修改 `content` 字段名为 `text`
- 更新 ODB 生成的 SQL 文件
- 修改 `comm.proto` 中 `MessageInfo` 结构
【验收标准】:
- 编译通过
- ODB 生成的 SQL 文件正确
- Protobuf 生成的代码正确
【依赖关系】: 无

### 任务 2: 更新 Protobuf 定义
【任务ID】: TASK-002
【任务名称】: 修改 comm.proto 和 message_store.proto
【优先级】: 高
【预估工时】: 1小时
【技术要点】:
- 修改 `MessageInfo` 中 `sender` 字段为 `sender_id`
- 重新生成 protobuf 代码
- 更新所有引用该字段的代码
【验收标准】:
- Protobuf 编译通过
- 所有引用更新完成
【依赖关系】: TASK-001

### 任务 3: 文件服务新增批量删除接口
【任务ID】: TASK-003
【任务名称】: 实现批量删除文件功能
【优先级】: 高
【预估工时】: 2小时
【技术要点】:
- 在 `file.proto` 中新增 `DeleteFileReq` 和 `DeleteFileRsp` 消息
- 新增 `DeleteMultiFile` RPC 接口
- 实现文件删除逻辑（延迟删除）
- 添加删除标记文件
【验收标准】:
- 接口定义正确
- 删除功能实现完整
- 支持批量操作
【依赖关系】: 无

### 任务 4: 实现本地事务表
【任务ID】: TASK-004
【任务名称】: 创建 message_outbox 表
【优先级】: 高
【预估工时】: 3小时
【技术要点】:
- 创建 `message_outbox` 表结构
- 实现 ODB 映射类
- 实现事务表的 CRUD 操作
- 实现定时任务扫描逻辑
【验收标准】:
- 表结构创建正确
- ODB 映射正确
- CRUD 操作正常
【依赖关系】: TASK-001

### 任务 5: 完善 PostFileMessages 函数
【任务ID】: TASK-005
【任务名称】: 实现文件消息存储功能
【优先级】: 高
【预估工时】: 4小时
【技术要点】:
- 实现文件消息的 MySQL 存储
- 实现文件消息的 ES 索引
- 实现事务表写入
- 实现重试机制（最多5次）
- 实现失败回滚逻辑
【验收标准】:
- 文件消息存储成功
- 事务表正确记录
- 重试机制正常工作
【依赖关系】: TASK-001, TASK-004

### 任务 6: 实现 DeleteTextMessages 接口
【任务ID】: TASK-006
【任务名称】: 实现文本消息删除功能
【优先级】: 高
【预估工时】: 3小时
【技术要点】:
- 从 MySQL 删除文本消息
- 从 ES 删除对应文档
- 使用事务保证原子性
- 实现乐观锁控制并发
【验收标准】:
- 删除操作正确
- 数据一致性保证
- 并发控制正常
【依赖关系】: TASK-001, TASK-002

### 任务 7: 实现 DeleteFileMessages 接口
【任务ID】: TASK-007
【任务名称】: 实现文件消息删除功能
【优先级】: 高
【预估工时】: 4小时
【技术要点】:
- 从 MySQL 删除文件消息
- 从 ES 删除对应文档
- 调用文件服务删除文件（延迟删除）
- 使用事务保证原子性
【验收标准】:
- 删除操作正确
- 文件服务调用正常
- 延迟删除机制正常
【依赖关系】: TASK-003, TASK-006

### 任务 8: 完善 MessageServiceImpl 接口
【任务ID】: TASK-008
【任务名称】: 完善 PostMessages 和 DeleteMessages 接口
【优先级】: 高
【预估工时】: 4小时
【技术要点】:
- 完善 `PostMessages` 接口实现
- 完善 `DeleteMessages` 接口实现
- 区分文本消息和文件消息的处理逻辑
- 集成事务表操作
【验收标准】:
- 接口功能完整
- 事务处理正确
- 错误处理完善
【依赖关系】: TASK-005, TASK-006, TASK-007

### 任务 9: 更新现有代码
【任务ID】: TASK-009
【任务名称】: 更新所有引用修改字段的代码
【优先级】: 中
【预估工时】: 3小时
【技术要点】:
- 更新 `messageDB.hpp` 中的时间处理逻辑
- 更新 `message_store_server.hpp` 中的消息处理
- 更新其他引用 `sender` 字段的代码
【验收标准】:
- 所有代码更新完成
- 编译通过
【依赖关系】: TASK-001, TASK-002

### 任务 10: 测试验证
【任务ID】: TASK-010
【任务名称】: 编写测试用例并验证
【优先级】: 中
【预估工时】: 4小时
【技术要点】:
- 编写单元测试
- 测试消息存储和删除功能
- 测试事务处理机制
- 测试并发场景
【验收标准】:
- 所有测试用例通过
- 边界条件覆盖完整
【依赖关系】: TASK-008, TASK-009

---

## 验收标准

### 功能验收
- [x] 时间戳字段修改正确，精度为毫秒级
- [x] `text` 字段重命名成功
- [x] `sender_id` 字段修改成功
- [x] 文件消息存储功能正常
- [x] 文本消息删除功能正常
- [x] 文件消息删除功能正常
- [x] 批量删除接口正常

### 性能验收
- [x] 单条消息存储时间 < 100ms
- [x] 批量删除100条消息时间 < 1s
- [x] 事务表扫描性能正常

### 安全验收
- [x] 乐观锁控制正常
- [x] 事务原子性保证
- [x] 错误处理完善

---

## 环境搭建

### 依赖库
- brpc (RPC框架)
- protobuf (协议定义)
- ODB (MySQL ORM)
- Redis (缓存)
- Elasticsearch (搜索引擎)
- spdlog (日志库)

### 编译命令
```bash
cmake -B build && cmake --build build
```

### 数据库准备
```sql
-- 创建 message_outbox 表
CREATE TABLE message_outbox (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    task_type       VARCHAR(32) NOT NULL,
    conversation_id VARCHAR(64) NOT NULL,
    msg_id          BIGINT UNSIGNED NOT NULL,
    payload         TEXT NOT NULL,
    status          TINYINT NOT NULL DEFAULT 0,
    retry_count     INT NOT NULL DEFAULT 0,
    max_retries     INT NOT NULL DEFAULT 5,
    next_retry_at   BIGINT NOT NULL DEFAULT 0,
    created_at      BIGINT NOT NULL,
    updated_at      BIGINT NOT NULL,
    last_error      TEXT,
    INDEX idx_status_retry (status, next_retry_at),
    INDEX idx_msg_id (msg_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

---

## TODO 清单

- [x] TASK-001: 修改 Message 数据结构
- [x] TASK-002: 更新 Protobuf 定义
- [x] TASK-003: 文件服务新增批量删除接口
- [x] TASK-004: 实现本地事务表
- [x] TASK-005: 完善 PostFileMessages 函数
- [x] TASK-006: 实现 DeleteTextMessages 接口
- [x] TASK-007: 实现 DeleteFileMessages 接口
- [x] TASK-008: 完善 MessageServiceImpl 接口
- [x] TASK-009: 更新现有代码
- [x] TASK-010: 测试验证
