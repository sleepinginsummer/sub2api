-- 独立于加列事务建索引，避免阻塞返利流水写入。
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_user_affiliate_ledger_operation_id
    ON user_affiliate_ledger (operation_id)
    WHERE operation_id IS NOT NULL;
