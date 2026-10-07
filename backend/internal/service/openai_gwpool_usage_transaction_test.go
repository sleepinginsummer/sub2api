package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolLegacyUsageProjectionDoesNotFreezeCache(t *testing.T) {
	account := gwpoolTestAccount(1)
	identity := openAIGatewayPoolCacheKey(account, gwpoolTestIdentity)
	now := time.Now().UTC()
	old := gatewayPoolUsageLedger{Tag: gatewayPoolLedgerTag(identity), UpdatedAt: now.Add(-time.Minute),
		Archived: map[string]GatewayPoolUsageArchive{"all": {Rounds: 1}}}
	account.Extra[gatewayPoolUsageExtraKey] = old
	account.Extra[gatewayPoolUsageTagKey] = old.Tag
	svc, repo := gatewayRuntimeService(account)
	first, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{account.ID})
	require.NoError(t, err)
	require.EqualValues(t, 1, first[account.ID].Runtime.Archived["all"].Rounds)
	_, cached := svc.codexCookies.poolUsageCache.Load(gatewayPoolUsageTag(identity))
	require.False(t, cached)
	// 无变化的前台记账也不能把临时迁移投影变成权威缓存。
	require.True(t, svc.changeGatewayPoolUsage(context.Background(), account, identity, func(*gatewayPoolUsageLedger) bool { return false }))
	_, cached = svc.codexCookies.poolUsageCache.Load(gatewayPoolUsageTag(identity))
	require.False(t, cached)
	old.UpdatedAt = now
	old.Archived = map[string]GatewayPoolUsageArchive{"all": {Rounds: 2}}
	require.NoError(t, repo.UpdateExtra(context.Background(), account.ID, map[string]any{gatewayPoolUsageExtraKey: old}))
	second, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{account.ID})
	require.NoError(t, err)
	require.EqualValues(t, 2, second[account.ID].Runtime.Archived["all"].Rounds)
}

// 模拟跨服务共享的事务入口；真正的事务锁和克隆行范围在 PostgreSQL 用例中验证。
type gatewayUsageTransactionRepo struct {
	*gatewayRuntimeRepo
	transactionMu sync.Mutex
	arrived       atomic.Int32
	both          chan struct{}
	pending       bool
	commitErr     error
}

func (r *gatewayUsageTransactionRepo) WithGatewayPoolUsageTransaction(ctx context.Context, _ string, _ int64,
	fn func(context.Context, AccountRepository) error,
) (bool, error) {
	if r.both != nil {
		if r.arrived.Add(1) == 2 {
			close(r.both)
		}
		select {
		case <-r.both:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	r.transactionMu.Lock()
	defer r.transactionMu.Unlock()
	if err := fn(ctx, r.gatewayRuntimeRepo); err != nil {
		return false, err
	}
	return !r.pending && r.commitErr == nil, r.commitErr
}

func TestGatewayPoolUsageTwoServicesKeepBothTickets(t *testing.T) {
	account := gwpoolTestAccount(1)
	identity := openAIGatewayPoolCacheKey(account, gwpoolTestIdentity)
	tag := gatewayPoolUsageTag(identity)
	now := time.Now().UTC()
	account.Extra[gatewayPoolUsageExtraKey] = gatewayPoolUsageLedger{Tag: tag, UpdatedAt: now.Add(-time.Minute)}
	account.Extra[gatewayPoolUsageTagKey] = tag
	repo := &gatewayUsageTransactionRepo{gatewayRuntimeRepo: &gatewayRuntimeRepo{account: *account}, both: make(chan struct{})}
	results := make(chan bool, 2)
	for _, ticket := range []string{"one", "two"} {
		go func(ticket string) {
			svc := &OpenAIGatewayService{accountRepo: repo}
			results <- svc.changeGatewayPoolUsage(context.Background(), account, identity, func(state *gatewayPoolUsageLedger) bool {
				return state.note("luna", ticket, now, true)
			})
		}(ticket)
	}
	require.True(t, <-results)
	require.True(t, <-results)
	fresh, err := repo.GetByID(context.Background(), account.ID)
	require.NoError(t, err)
	state := readGatewayPoolUsage(fresh, tag)
	require.Len(t, state.Rounds, 1)
	require.Equal(t, 2, state.Rounds[0].Attempted)
	require.Contains(t, state.Rounds[0].Tickets, "one")
	require.Contains(t, state.Rounds[0].Tickets, "two")
}

func TestGatewayPoolUsageCacheFollowsNewerCloneSnapshot(t *testing.T) {
	account, clone := gwpoolTestAccount(1), gwpoolTestAccount(2)
	repo := gatewayUsageConfigRepo{gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account, *clone}}}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	identity, err := svc.codexCookies.gatewayPoolIdentity(context.Background(), account)
	require.NoError(t, err)
	tag := gatewayPoolUsageTag(identity)
	now := time.Now().UTC()
	legacy := gatewayPoolUsageLedger{Tag: gatewayPoolLedgerTag(identity), UpdatedAt: now.Add(time.Hour),
		Archived: map[string]GatewayPoolUsageArchive{"all": {Rounds: 99}}}
	require.NoError(t, repo.UpdateExtra(context.Background(), account.ID, map[string]any{gatewayPoolUsageExtraKey: legacy, gatewayPoolUsageTagKey: legacy.Tag}))
	newer := gatewayPoolUsageLedger{Tag: tag, UpdatedAt: now, Archived: map[string]GatewayPoolUsageArchive{"all": {Rounds: 1}}}
	require.NoError(t, repo.UpdateExtra(context.Background(), clone.ID, map[string]any{gatewayPoolUsageExtraKey: newer, gatewayPoolUsageTagKey: tag}))
	first, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{account.ID})
	require.NoError(t, err)
	require.EqualValues(t, 1, first[account.ID].Runtime.Archived["all"].Rounds)
	oldCached, ok := svc.codexCookies.poolUsageCache.Load(tag)
	require.True(t, ok)
	newer.UpdatedAt = now.Add(time.Second)
	newer.Archived = map[string]GatewayPoolUsageArchive{"all": {Rounds: 2}}
	require.NoError(t, repo.UpdateExtra(context.Background(), clone.ID, map[string]any{gatewayPoolUsageExtraKey: newer}))
	second, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{account.ID})
	require.NoError(t, err)
	require.EqualValues(t, 2, second[account.ID].Runtime.Archived["all"].Rounds)
	// 模拟迟到的提交/展示读回发布，缓存也不能倒退到第一次读取的旧值。
	oldState, valid := oldCached.(*gatewayPoolUsageLedger)
	require.True(t, valid)
	svc.cacheGatewayPoolUsage(tag, oldState)
	cached, ok := svc.codexCookies.poolUsageCache.Load(tag)
	require.True(t, ok)
	state, valid := cached.(*gatewayPoolUsageLedger)
	require.True(t, valid)
	require.EqualValues(t, 2, state.Archived["all"].Rounds)
}

func TestGatewayPoolUsageTransactionViewDoesNotPoisonCache(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	identity, err := svc.codexCookies.gatewayPoolIdentity(context.Background(), account)
	require.NoError(t, err)
	tag := gatewayPoolUsageTag(identity)
	now := time.Now().UTC()
	committed := gatewayPoolUsageLedger{Tag: tag, UpdatedAt: now, Archived: map[string]GatewayPoolUsageArchive{"all": {Rounds: 1}}}
	require.NoError(t, repo.UpdateExtra(context.Background(), account.ID, map[string]any{gatewayPoolUsageExtraKey: committed, gatewayPoolUsageTagKey: tag}))
	_, err = svc.GatewayPoolRuntimeProgress(context.Background(), []int64{account.ID})
	require.NoError(t, err)
	// 轻量仓储模拟事务私有视图；只使用 Tx 上下文标记，不执行这个占位事务。
	pending := gatewayPoolUsageLedger{Tag: tag, UpdatedAt: now.Add(time.Second), Archived: map[string]GatewayPoolUsageArchive{"all": {Rounds: 2}}}
	require.NoError(t, repo.UpdateExtra(context.Background(), account.ID, map[string]any{gatewayPoolUsageExtraKey: pending}))
	txCtx := dbent.NewTxContext(context.Background(), &dbent.Tx{})
	view, err := svc.GatewayPoolRuntimeProgress(txCtx, []int64{account.ID})
	require.NoError(t, err)
	require.EqualValues(t, 2, view[account.ID].Runtime.Archived["all"].Rounds)
	cached, exists := svc.codexCookies.poolUsageCache.Load(tag)
	require.True(t, exists)
	state, valid := cached.(*gatewayPoolUsageLedger)
	require.True(t, valid)
	require.EqualValues(t, 1, state.Archived["all"].Rounds)
	// 回滚后普通展示仍读到原来的已提交状态。
	require.NoError(t, repo.UpdateExtra(context.Background(), account.ID, map[string]any{gatewayPoolUsageExtraKey: committed}))
	view, err = svc.GatewayPoolRuntimeProgress(context.Background(), []int64{account.ID})
	require.NoError(t, err)
	require.EqualValues(t, 1, view[account.ID].Runtime.Archived["all"].Rounds)
}

func TestGatewayPoolUsageDoesNotPublishBeforeCommit(t *testing.T) {
	for _, pending := range []bool{true, false} {
		name := "commit_failed"
		if pending {
			name = "caller_transaction"
		}
		t.Run(name, func(t *testing.T) {
			account := gwpoolTestAccount(1)
			identity := openAIGatewayPoolCacheKey(account, gwpoolTestIdentity)
			repo := &gatewayUsageTransactionRepo{gatewayRuntimeRepo: &gatewayRuntimeRepo{account: *account}, pending: pending}
			if !pending {
				repo.commitErr = errors.New("commit failed")
			}
			svc := &OpenAIGatewayService{accountRepo: repo}
			run := svc.startGatewayPoolProgress(context.Background(), account, identity)
			require.Zero(t, run.progress.Sequence, "未提交的验证序号不能发布")
			_, cached := svc.codexCookies.poolUsageCache.Load(gatewayPoolUsageTag(identity))
			require.False(t, cached)
		})
	}
}
