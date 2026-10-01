package repository

import (
	"context"
	"testing"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestAPIKeyRepositoryCreateWithLimit(t *testing.T) {
	repo, client := newAPIKeyRepoSQLite(t)
	ctx := context.Background()
	u := mustCreateAPIKeyRepoUser(t, ctx, client, "create-limit@test.com")
	key := func(value string) *service.APIKey {
		return &service.APIKey{UserID: u.ID, Key: value, Name: value, Status: service.StatusActive}
	}
	first := key("sk-limit-first")
	require.NoError(t, repo.CreateWithLimit(ctx, first, 1))
	require.ErrorIs(t, repo.CreateWithLimit(ctx, key("sk-limit-second"), 1), service.ErrAPIKeyCountExceeded)
	require.NoError(t, repo.Delete(ctx, first.ID))
	require.NoError(t, repo.CreateWithLimit(ctx, key("sk-limit-second"), 1), "软删除应释放名额")
	require.NoError(t, repo.CreateWithLimit(ctx, key("sk-unlimited"), 0), "禁用上限保持普通创建行为")
	require.ErrorIs(t, repo.CreateWithLimit(ctx, key("sk-unlimited"), 0), service.ErrAPIKeyExists)
}

func TestAPIKeyRepositoryCreateWithLimitUsesCallerTransaction(t *testing.T) {
	repo, client := newAPIKeyRepoSQLite(t)
	ctx := context.Background()
	u := mustCreateAPIKeyRepoUser(t, ctx, client, "create-limit-tx@test.com")
	tx, err := client.Tx(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	key := &service.APIKey{UserID: u.ID, Key: "sk-limit-rollback", Name: "rollback", Status: service.StatusActive}
	require.NoError(t, repo.CreateWithLimit(dbent.NewTxContext(ctx, tx), key, 1))
	require.NoError(t, tx.Rollback(), "仓储不能提前提交调用方的事务")
	count, err := repo.CountByUserID(ctx, u.ID)
	require.NoError(t, err)
	require.Zero(t, count)
}
