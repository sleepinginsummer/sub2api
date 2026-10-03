//go:build integration

package repository

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// 在真实 PostgreSQL 的临时热表上执行新增迁移，检查扫描状态与锁等待边界。
func TestGatewayPoolMigration249PreservesHotTableSafety(t *testing.T) {
	ctx := context.Background()
	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `CREATE TEMP TABLE review_usage_logs (
		request_type smallint NOT NULL,
		CONSTRAINT usage_logs_request_type_check CHECK (request_type BETWEEN 0 AND 6) NOT VALID
	)`)
	require.NoError(t, err)
	body, err := os.ReadFile("../../migrations/249_allow_gwpool_degraded_usage_request_type.sql")
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, strings.ReplaceAll(string(body), "ALTER TABLE usage_logs", "ALTER TABLE review_usage_logs"))
	require.NoError(t, err)
	var validated bool
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT convalidated FROM pg_constraint
		WHERE conrelid = 'review_usage_logs'::regclass AND conname = 'usage_logs_request_type_check'`).Scan(&validated))
	var lockTimeout string
	require.NoError(t, tx.QueryRowContext(ctx, "SHOW lock_timeout").Scan(&lockTimeout))
	t.Logf("migration 249: convalidated=%t, lock_timeout=%s", validated, lockTimeout)
	require.False(t, validated, "仅放宽已有 CHECK 不应在 ACCESS EXCLUSIVE 锁内扫描整个用量表")
	require.NotEqual(t, "0", lockTimeout, "新迁移不能恢复无限锁等待")
}
