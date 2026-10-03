//go:build integration

package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func gatewayHistoryIntegrationDelta(gateway string, at time.Time) map[string]any {
	return map[string]any{service.OpenAIGatewayHistoryExtraKey: map[string]any{
		"current": gateway, "updated_at": at.Format(time.RFC3339Nano),
		"seen": map[string]any{gateway: map[string]any{"at": at.Format(time.RFC3339Nano), "verdict": "full"}},
	}}
}

func TestGatewayHistoryConcurrentRepositoriesKeepAllLandings(t *testing.T) {
	ctx := context.Background()
	acct := mustCreateAccount(t, integrationEntClient, &service.Account{Name: "history-concurrent", Platform: service.PlatformOpenAI, Extra: map[string]any{"unrelated": "preserved"}})
	t.Cleanup(func() { require.NoError(t, integrationEntClient.Account.DeleteOneID(acct.ID).Exec(ctx)) })
	start := make(chan struct{})
	results := make(chan error, 16)
	now := time.Now().UTC()
	for i := 0; i < 16; i++ {
		go func(index int) {
			<-start
			repo := newAccountRepositoryWithSQL(integrationEntClient, integrationDB, nil)
			results <- repo.UpdateExtra(ctx, acct.ID, gatewayHistoryIntegrationDelta(fmt.Sprintf("unified-%d", index), now.Add(time.Duration(index)*time.Millisecond)))
		}(i)
	}
	close(start)
	for i := 0; i < 16; i++ {
		require.NoError(t, <-results)
	}
	repo := newAccountRepositoryWithSQL(integrationEntClient, integrationDB, nil)
	saved, err := repo.GetByID(ctx, acct.ID)
	require.NoError(t, err)
	history := saved.Extra[service.OpenAIGatewayHistoryExtraKey].(map[string]any)
	require.Len(t, history["seen"].(map[string]any), 16)
	require.Equal(t, "unified-15", history["current"], "迟到的旧请求不能倒退当前落点")
	require.Equal(t, "preserved", saved.Extra["unrelated"])
	require.NotContains(t, history, "pool_free", "没有新清单时不能伪造剩余数")
}

func TestGatewayHistoryLockSurvivesCallerTransaction(t *testing.T) {
	ctx := context.Background()
	acct := mustCreateAccount(t, integrationEntClient, &service.Account{Name: "history-transaction", Platform: service.PlatformOpenAI})
	t.Cleanup(func() { require.NoError(t, integrationEntClient.Account.DeleteOneID(acct.ID).Exec(ctx)) })
	repo := newAccountRepositoryWithSQL(integrationEntClient, integrationDB, nil)
	tx, err := integrationEntClient.Tx(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	now := time.Now().UTC()
	require.NoError(t, repo.UpdateExtra(dbent.NewTxContext(ctx, tx), acct.ID, gatewayHistoryIntegrationDelta("unified-142", now)))
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		other := newAccountRepositoryWithSQL(integrationEntClient, integrationDB, nil)
		done <- other.UpdateExtra(waitCtx, acct.ID, gatewayHistoryIntegrationDelta("unified-84", now.Add(time.Second)))
	}()
	select {
	case err := <-done:
		t.Fatalf("调用方提交前不应完成历史合并: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, <-done)
	saved, err := repo.GetByID(ctx, acct.ID)
	require.NoError(t, err)
	history := saved.Extra[service.OpenAIGatewayHistoryExtraKey].(map[string]any)
	require.Len(t, history["seen"].(map[string]any), 2)
}
