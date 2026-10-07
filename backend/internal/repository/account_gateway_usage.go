package repository

import (
	"context"
	"fmt"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

// WithGatewayPoolUsageTransaction 按配置周期串行化克隆域的读取和写入。
// 返回值只表示自有事务已提交；调用方事务的锁持续到其提交/回滚，不能提前发布缓存。
func (r *accountRepository) WithGatewayPoolUsageTransaction(ctx context.Context, tag string, accountID int64,
	fn func(context.Context, service.AccountRepository) error,
) (bool, error) {
	if tag == "" || accountID <= 0 || fn == nil {
		return false, fmt.Errorf("invalid gateway usage transaction")
	}
	baseCtx := ctx
	tx := dbent.TxFromContext(ctx)
	owned := tx == nil
	if owned {
		var err error
		tx, err = r.client.Tx(ctx)
		if err != nil {
			return false, err
		}
		ctx = dbent.NewTxContext(ctx, tx)
	}
	client := tx.Client()
	defer func() {
		if owned {
			_ = tx.Rollback()
		}
	}()
	// 直接等待数据库锁，让 ctx 限制排队时间；不叠加不可取消的进程内仓储锁。
	locks, err := client.QueryContext(ctx, "SELECT pg_advisory_xact_lock($1)", advisoryLockHash("gwpool-usage:"+tag))
	if err != nil {
		return false, err
	}
	if err := locks.Close(); err != nil {
		return false, err
	}
	// 防止验证身份后账号配置被并发改写；不同克隆仍由上面的周期锁串行化。
	rows, err := client.QueryContext(ctx, "SELECT id FROM accounts WHERE id = $1 AND deleted_at IS NULL FOR UPDATE", accountID)
	if err != nil {
		return false, err
	}
	found := rows.Next()
	rowErr := rows.Err()
	closeErr := rows.Close()
	if rowErr != nil {
		return false, rowErr
	}
	if closeErr != nil {
		return false, closeErr
	}
	if !found {
		return false, service.ErrAccountNotFound
	}
	// 绑定事务 client 和执行器，避免 GetByID/关联字段仍从事务外读取。
	scoped := newAccountRepositoryWithSQL(client, client, nil)
	if err := fn(ctx, scoped); err != nil {
		return false, err
	}
	if !owned {
		return false, nil
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	r.syncSchedulerAccountSnapshot(baseCtx, accountID)
	return true, nil
}
