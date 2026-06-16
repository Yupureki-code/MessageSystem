# 消息存储服务测试

## 测试框架

使用 Google Test (gtest) 进行单元测试。

## 测试文件

| 文件 | 描述 |
|------|------|
| test_message_store.cpp | 消息存储服务接口测试 |
| test_database.cpp | 数据库操作测试 |

## 测试用例

### 消息存储服务测试 (test_message_store)

| 测试用例 | 描述 |
|---------|------|
| PostSingleTextMessage | 测试发布单条文本消息 |
| PostBatchTextMessages | 测试批量发布文本消息 |
| GetRecentMessages | 测试获取最近消息 |
| GetHistoryMessages | 测试获取历史消息 |
| SearchMessages | 测试消息搜索 |
| DeleteTextMessage | 测试删除文本消息 |
| BatchDeleteMessages | 测试批量删除消息 |
| DeleteNonexistentMessage | 测试删除不存在的消息 |
| EmptyMessageList | 测试空消息列表处理 |
| MessageIdUniqueness | 测试消息ID唯一性 |

### 数据库操作测试 (test_database)

| 测试用例 | 描述 |
|---------|------|
| InsertSingleTextMessage | 测试插入单条文本消息 |
| InsertBatchTextMessages | 测试批量插入文本消息 |
| GetRecentMessages | 测试获取最近消息 |
| GetHistoryMessages | 测试获取历史消息 |
| DeleteMessage | 测试删除消息 |
| RemoveConversation | 测试删除会话所有消息 |
| OutboxInsert | 测试事务表插入 |
| OutboxBatchInsert | 测试事务表批量插入 |
| GetPendingTasks | 测试获取待处理任务 |
| MarkCompleted | 测试标记任务完成 |
| MarkFailed | 测试标记任务失败 |

## 前置条件

1. 安装依赖:
```bash
sudo apt-get install libgtest-dev cmake g++ libmysqlclient-dev
```

2. 配置数据库环境变量 (可选):
```bash
export MYSQL_HOST=127.0.0.1
export MYSQL_USER=test
export MYSQL_PASSWORD=test123
export MYSQL_DB=test
export MYSQL_PORT=3306
```

3. 启动消息存储服务 (用于服务测试):
```bash
./message_store_server
```

## 运行测试

### 方法1: 使用脚本
```bash
cd test/gtest
chmod +x run_test.sh

# 运行所有测试
./run_test.sh

# 运行服务测试（需要提供服务地址）
./run_test.sh 127.0.0.1:8003
```

### 方法2: 手动运行
```bash
cd test/gtest
mkdir -p build
cd build
cmake ..
make -j$(nproc)

# 运行数据库测试
./test_database

# 运行服务测试
./test_message_store 127.0.0.1:8003
```

### 方法3: 使用ctest
```bash
cd test/gtest/build
ctest --output-on-failure
```

## 测试输出示例

```
[==========] Running 11 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 11 tests from DatabaseTest
[ RUN      ] DatabaseTest.InsertSingleTextMessage
[       OK ] DatabaseTest.InsertSingleTextMessage (5 ms)
[ RUN      ] DatabaseTest.InsertBatchTextMessages
[       OK ] DatabaseTest.InsertBatchTextMessages (8 ms)
...
[==========] 11 tests from 1 test suite ran. (150 ms total)
[  PASSED  ] 11 tests.
```

## 注意事项

1. 测试会自动创建唯一的会话ID，避免测试数据冲突
2. 数据库测试会直接操作数据库，测试数据需要手动清理
3. 消息搜索测试需要等待ES索引更新（约1秒）
4. 服务测试需要消息存储服务正在运行
