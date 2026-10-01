//go:build integration

package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/ent/apikey"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func createLimitIntegrationUser(t *testing.T) (context.Context, *service.User) {
	t.Helper()
	ctx := context.Background()
	u, err := integrationEntClient.User.Create().SetEmail(uuid.NewString() + "@key-limit.test").SetPasswordHash("test").SetRole(service.RoleUser).SetStatus(service.StatusActive).Save(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := integrationEntClient.APIKey.Delete().Where(apikey.UserIDEQ(u.ID)).Exec(ctx)
		require.NoError(t, err)
		require.NoError(t, integrationEntClient.User.DeleteOneID(u.ID).Exec(ctx))
	})
	return ctx, userEntityToService(u)
}

func TestAPIKeyCreateWithLimitConcurrentRepositories(t *testing.T) {
	ctx, u := createLimitIntegrationUser(t)
	repo := newAPIKeyRepositoryWithSQL(integrationEntClient, integrationDB)
	require.NoError(t, repo.Create(ctx, &service.APIKey{UserID: u.ID, Key: uuid.NewString(), Name: "existing", Status: service.StatusActive}))
	start := make(chan struct{})
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func(i int) {
			<-start
			other := newAPIKeyRepositoryWithSQL(integrationEntClient, integrationDB)
			results <- other.CreateWithLimit(ctx, &service.APIKey{UserID: u.ID, Key: uuid.NewString(), Name: fmt.Sprintf("concurrent-%d", i), Status: service.StatusActive}, 2)
		}(i)
	}
	close(start)
	admitted := 0
	for i := 0; i < 8; i++ {
		err := <-results
		if err == nil {
			admitted++
		} else {
			require.ErrorIs(t, err, service.ErrAPIKeyCountExceeded)
		}
	}
	require.Equal(t, 1, admitted, "最后一个名额只能被一个创建请求取得")
	count, err := repo.CountByUserID(ctx, u.ID)
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
}

// 调用方事务返回后，进程锁已释放；第二个仓储仍须由 PostgreSQL 事务锁挡到提交后再复查数量。
func TestAPIKeyCreateWithLimitLockSurvivesMethodReturn(t *testing.T) {
	ctx, u := createLimitIntegrationUser(t)
	repo := newAPIKeyRepositoryWithSQL(integrationEntClient, integrationDB)
	tx, err := integrationEntClient.Tx(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, repo.CreateWithLimit(dbent.NewTxContext(ctx, tx), &service.APIKey{UserID: u.ID, Key: uuid.NewString(), Name: "pending", Status: service.StatusActive}, 1))
	done := make(chan error, 1)
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go func() {
		other := newAPIKeyRepositoryWithSQL(integrationEntClient, integrationDB)
		done <- other.CreateWithLimit(waitCtx, &service.APIKey{UserID: u.ID, Key: uuid.NewString(), Name: "blocked", Status: service.StatusActive}, 1)
	}()
	select {
	case err := <-done:
		t.Fatalf("事务提交前不应完成创建: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, tx.Commit())
	require.ErrorIs(t, <-done, service.ErrAPIKeyCountExceeded)
}
