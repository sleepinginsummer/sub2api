-- 管理员登记线下提现的幂等标识：同一 operation_id 只对应一条流水，
-- 重试同一次登记时据此返回首次登记的结果，不重复扣减。
SET LOCAL lock_timeout = '5s';

ALTER TABLE user_affiliate_ledger
    ADD COLUMN IF NOT EXISTS operation_id VARCHAR(64);

COMMENT ON COLUMN user_affiliate_ledger.operation_id IS '管理员登记线下提现的幂等标识（由 Idempotency-Key 派生的 SHA-256 十六进制串）；其他流水为 NULL';
