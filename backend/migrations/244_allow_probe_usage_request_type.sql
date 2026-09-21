-- 292 猎手探测记账：usage_logs.request_type 新增 6（probe）。
-- usage_logs 是热表：锁等待必须有界，新约束先只校验后续写入，避免启动迁移扫描历史数据。
SET LOCAL lock_timeout = '5s';

ALTER TABLE usage_logs
    DROP CONSTRAINT IF EXISTS usage_logs_request_type_check;

ALTER TABLE usage_logs
    ADD CONSTRAINT usage_logs_request_type_check
    CHECK (request_type >= 0 AND request_type <= 6) NOT VALID;
