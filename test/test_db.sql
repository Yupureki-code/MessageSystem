-- 测试SQL：验证表结构
-- 在MySQL中执行以下SQL进行验证

-- 1. 验证message表结构
DESCRIBE message;

-- 2. 验证message_outbox表结构
DESCRIBE message_outbox;

-- 3. 验证索引
SHOW INDEX FROM message;
SHOW INDEX FROM message_outbox;

-- 4. 插入测试数据到message表
INSERT INTO message (message_id, conversation_id, sender_id, message_type, create_time, text) 
VALUES ('test_msg_001', 'conv_test_001', 'user_001', 0, 1718544000000, '测试消息内容');

-- 5. 查询测试数据
SELECT * FROM message WHERE conversation_id = 'conv_test_001';

-- 6. 插入测试数据到message_outbox表
INSERT INTO message_outbox (task_type, conversation_id, msg_id, payload, status, retry_count, max_retries, next_retry_at, created_at, updated_at) 
VALUES ('INDEX_ES', 'conv_test_001', 1, '{"message_id":"test_msg_001","text":"测试消息内容"}', 0, 0, 5, 1718544000000, 1718544000000, 1718544000000);

-- 7. 查询事务表数据
SELECT * FROM message_outbox WHERE conversation_id = 'conv_test_001';

-- 8. 清理测试数据
DELETE FROM message WHERE message_id = 'test_msg_001';
DELETE FROM message_outbox WHERE conversation_id = 'conv_test_001';
