package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type gatewayUsageConfigRepo struct {
	gatewayRotationRepo
}

func (r gatewayUsageConfigRepo) GetByIDs(ctx context.Context, ids []int64) ([]*Account, error) {
	accounts := make([]*Account, 0, len(ids))
	for _, id := range ids {
		account, err := r.GetByID(ctx, id)
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, account)
	}
	return accounts, nil
}

func TestGatewayPoolPreviousHistoryResetSurvivesPersistence(t *testing.T) {
	for _, clear := range []bool{true, false} {
		name := "reset"
		if clear {
			name = "clear"
		}
		t.Run(name, func(t *testing.T) {
			account := gwpoolTestAccount(1)
			svc, repo := gatewayRuntimeService(account)
			identity, err := svc.codexCookies.gatewayPoolIdentity(context.Background(), account)
			require.NoError(t, err)
			at := time.Now().UTC().Add(-time.Minute)
			tag := gatewayPoolLedgerTag(identity)
			previous := openAIGatewayHistory{LedgerTag: tag, UpdatedAt: at,
				Seen: map[string]openAIGatewaySeen{"g": {At: at, Cooldown: resetTestCooldown(at)}}}
			original := openAIGatewayHistory{LedgerTag: "other-member", UpdatedAt: at.Add(time.Second), Previous: &previous,
				Seen: map[string]openAIGatewaySeen{"other": {At: at}}}
			repo.account.Extra[OpenAIGatewayHistoryExtraKey] = original
			now := time.Now().UTC()
			if clear {
				require.NoError(t, svc.writeGatewayPoolCooldownClear(context.Background(), 1, identity, now, false))
			} else {
				require.NoError(t, svc.writeGatewayPoolCooldownReset(context.Background(), 1, identity, now, now, false))
			}
			// 迟到的旧快照也不能恢复已清理/重置的上一成员学习。
			require.NoError(t, repo.UpdateExtra(context.Background(), 1, map[string]any{OpenAIGatewayHistoryExtraKey: original}))
			fresh, err := repo.GetByID(context.Background(), 1)
			require.NoError(t, err)
			history, ok := readOpenAIGatewayHistory(fresh)
			require.True(t, ok)
			require.Equal(t, "other-member", history.LedgerTag)
			require.Contains(t, history.Seen, "other")
			require.NotNil(t, history.Previous)
			require.Nil(t, history.Previous.Previous)
			require.Equal(t, tag, fresh.Extra[openAIGatewayPreviousLedgerTagExtraKey])
			if clear {
				require.Equal(t, now, history.Previous.CooldownReset.ClearedAt)
				require.True(t, history.Previous.Seen["g"].Cooldown.Cleared)
			} else {
				require.Equal(t, now, history.Previous.CooldownReset.LastAt)
				require.Equal(t, now, history.Previous.Seen["g"].Cooldown.ResetAt)
			}
		})
	}
}

func TestGatewayPoolUsageCycleSeparatesConfigurations(t *testing.T) {
	a, b := gwpoolTestAccount(1), gwpoolTestAccount(2)
	first := newGwpoolFakePool(t, "offline", 150)
	second := newGwpoolFakePool(t, "offline", 150)
	first.configure(a)
	second.configure(b)
	clone := *first.account(3)
	repo := gatewayUsageConfigRepo{gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*a, *b, clone}}}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	identity, err := svc.codexCookies.gatewayPoolIdentity(context.Background(), a)
	require.NoError(t, err)
	other, err := svc.codexCookies.gatewayPoolIdentity(context.Background(), b)
	require.NoError(t, err)
	require.NotEqual(t, gatewayPoolUsageTag(identity), gatewayPoolUsageTag(other))
	require.Equal(t, gatewayPoolLedgerTag(identity), gatewayPoolLedgerTag(other))
	now := time.Now().UTC()
	svc.codexCookies.poolPairs.Store(identity, openAIGatewayPoolPair{cookie: "offline", gateway: "g", version: "v", until: now.Add(time.Hour), routeExpiresAt: now.Add(time.Hour)})
	svc.codexCookies.gatewayPoolMarkVerifiedFull(identity, "v", "luna")
	ctx, finish := svc.beginGatewayPoolUsageRequest(context.Background(), a)
	defer finish()
	gatewayPoolUsageMarkSending(ctx, identity)
	require.True(t, svc.noteGatewayPoolFullUse(ctx, a, identity, OpenAIGatewayPoolApplied{AccountID: a.ID, Gateway: "g", Version: "v"}, now))
	fresh, err := repo.GetByID(context.Background(), a.ID)
	require.NoError(t, err)
	state := readGatewayPoolUsage(fresh, gatewayPoolUsageTag(identity))
	require.Len(t, state.Rounds, 1)
	require.True(t, state.Rounds[0].EndedAt.IsZero())
	// 即使 B 有自己的旧周期，B 的空闲维护也只能结束 B。
	require.True(t, svc.changeGatewayPoolUsage(context.Background(), b, other, func(state *gatewayPoolUsageLedger) bool {
		return state.note("luna", "b-ticket", now.Add(-time.Hour), true)
	}))
	freshB, err := repo.GetByID(context.Background(), b.ID)
	require.NoError(t, err)
	svc.maintainGatewayPoolUsage(context.Background(), freshB, now)
	freshB, err = repo.GetByID(context.Background(), b.ID)
	require.NoError(t, err)
	require.False(t, readGatewayPoolUsage(freshB, gatewayPoolUsageTag(other)).Rounds[0].EndedAt.IsZero())
	svc.finishGatewayPoolUsageIfExhausted(context.Background(), b)
	views, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{a.ID, b.ID, clone.ID})
	require.NoError(t, err)
	require.Len(t, views[a.ID].Runtime.Rounds, 1)
	require.True(t, views[a.ID].Runtime.Rounds[0].EndedAt.IsZero())
	require.Equal(t, views[a.ID].Runtime.Rounds[0].ID, views[clone.ID].Runtime.Rounds[0].ID, "同配置克隆仍共享周期")
	require.NotEqual(t, views[a.ID].Runtime.Rounds[0].ID, views[b.ID].Runtime.Rounds[0].ID)
}

func TestGatewayPoolUsageUpgradeAdoptsLatestLegacyCloneOnce(t *testing.T) {
	account, clone := gwpoolTestAccount(1), gwpoolTestAccount(2)
	identity := openAIGatewayPoolCacheKey(account, gwpoolTestIdentity)
	legacy := gatewayPoolUsageLedger{Tag: gatewayPoolLedgerTag(identity), UpdatedAt: time.Now().UTC().Add(-time.Hour),
		Archived: map[string]GatewayPoolUsageArchive{"all": {Rounds: 7, DurationMS: 777}}}
	clone.Extra[gatewayPoolUsageExtraKey] = legacy
	clone.Extra[gatewayPoolUsageTagKey] = legacy.Tag
	repo := gatewayUsageConfigRepo{gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account, *clone}}}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	views, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{account.ID})
	require.NoError(t, err)
	require.Equal(t, legacy.Archived, views[account.ID].Runtime.Archived, "没有本地旧快照的克隆也要恢复共享历史")
	require.True(t, svc.changeGatewayPoolUsage(context.Background(), account, identity, func(state *gatewayPoolUsageLedger) bool {
		return state.note("luna", "new-ticket", time.Now().UTC(), true)
	}))
	// 已持久化的新配置域不能再次被旧版本写出的共享快照覆盖。
	legacy.UpdatedAt = time.Now().UTC().Add(time.Minute)
	legacy.Archived = map[string]GatewayPoolUsageArchive{"all": {Rounds: 99}}
	require.NoError(t, repo.UpdateExtra(context.Background(), clone.ID, map[string]any{gatewayPoolUsageExtraKey: legacy}))
	require.True(t, svc.changeGatewayPoolUsage(context.Background(), account, identity, func(state *gatewayPoolUsageLedger) bool {
		return state.note("luna", "next-ticket", time.Now().UTC(), true)
	}))
	restarted := &OpenAIGatewayService{accountRepo: repo}
	views, err = restarted.GatewayPoolRuntimeProgress(context.Background(), []int64{clone.ID})
	require.NoError(t, err)
	require.EqualValues(t, 7, views[clone.ID].Runtime.Archived["all"].Rounds, "冷启动也优先采用已经持久化的配置域")
	require.True(t, restarted.changeGatewayPoolUsage(context.Background(), clone, identity, func(state *gatewayPoolUsageLedger) bool {
		return state.note("luna", "clone-ticket", time.Now().UTC(), true)
	}))
	freshClone, err := repo.GetByID(context.Background(), clone.ID)
	require.NoError(t, err)
	require.EqualValues(t, 7, readGatewayPoolUsage(freshClone, gatewayPoolUsageTag(identity)).Archived["all"].Rounds)
	fresh, err := repo.GetByID(context.Background(), account.ID)
	require.NoError(t, err)
	state := readGatewayPoolUsage(fresh, gatewayPoolUsageTag(identity))
	require.EqualValues(t, 7, state.Archived["all"].Rounds)
	require.EqualValues(t, 7, state.Previous.Archived["all"].Rounds)
}

func TestGatewayPoolUsageConfigurationSwitchKeepsPreviousCycle(t *testing.T) {
	account := gwpoolTestAccount(1)
	oldIdentity := openAIGatewayPoolCacheKey(account, gwpoolTestIdentity)
	at := time.Now().UTC().Add(-time.Hour)
	legacy := gatewayPoolUsageLedger{Tag: gatewayPoolLedgerTag(oldIdentity), UpdatedAt: at.Add(-time.Hour),
		Archived: map[string]GatewayPoolUsageArchive{"all": {Rounds: 5}}}
	old := gatewayPoolUsageLedger{Tag: gatewayPoolUsageTag(oldIdentity), UpdatedAt: at, Previous: &legacy,
		Archived: map[string]GatewayPoolUsageArchive{"all": {Rounds: 9}}}
	account.Extra[gatewayPoolUsageExtraKey] = old
	account.Extra[gatewayPoolUsageTagKey] = old.Tag
	account.Extra[gatewayPoolUsagePreviousTagKey] = legacy.Tag
	newGwpoolFakePool(t, "offline", 150).configure(account)
	identity := openAIGatewayPoolCacheKey(account, gwpoolTestIdentity)
	require.NotEqual(t, old.Tag, gatewayPoolUsageTag(identity))
	svc, repo := gatewayRuntimeService(account)
	require.True(t, svc.changeGatewayPoolUsage(context.Background(), account, identity, func(state *gatewayPoolUsageLedger) bool {
		return state.note("luna", "new-config-ticket", time.Now().UTC(), true)
	}))
	fresh, err := repo.GetByID(context.Background(), account.ID)
	require.NoError(t, err)
	state := readGatewayPoolUsage(fresh, gatewayPoolUsageTag(identity))
	require.NotNil(t, state.Previous)
	require.Equal(t, old.Tag, state.Previous.Tag, "迁移旧共享统计不能顶掉刚切出的配置周期")
	require.Equal(t, old.Archived, state.Previous.Archived)
	require.Nil(t, state.Previous.Previous)
}

func TestGatewayPoolUsageConfigurationUpgradeKeepsHistory(t *testing.T) {
	account := gwpoolTestAccount(1)
	identity := openAIGatewayPoolCacheKey(account, gwpoolTestIdentity)
	at := time.Now().UTC().Add(-time.Hour)
	old := gatewayPoolUsageLedger{Tag: gatewayPoolLedgerTag(identity), UpdatedAt: at,
		Archived: map[string]GatewayPoolUsageArchive{"all": {Rounds: 5, DurationMS: 12345}}}
	old.note("luna", "old-ticket", at, true)
	old.end(at.Add(time.Minute))
	account.Extra[gatewayPoolUsageExtraKey] = old
	migrated := readGatewayPoolUsageForIdentity(account, identity)
	require.Equal(t, gatewayPoolUsageTag(identity), migrated.Tag)
	require.Equal(t, old.Archived, migrated.Archived)
	require.Equal(t, old.Rounds, migrated.Rounds)
	require.NotNil(t, migrated.Previous)
	delete(migrated.Rounds[0].Tickets, "old-ticket")
	require.NotEmpty(t, migrated.Previous.Rounds[0].Tickets, "旧快照与新周期不能共享可变 map")
	svc, repo := gatewayRuntimeService(account)
	require.True(t, svc.changeGatewayPoolUsage(context.Background(), account, identity, func(state *gatewayPoolUsageLedger) bool {
		return state.note("luna", "new-ticket", time.Now().UTC(), true)
	}))
	fresh, err := repo.GetByID(context.Background(), account.ID)
	require.NoError(t, err)
	durable := readGatewayPoolUsage(fresh, gatewayPoolUsageTag(identity))
	require.Equal(t, old.Archived, durable.Archived)
	require.Len(t, durable.Rounds, 2)
	require.Equal(t, old.Tag, durable.Previous.Tag)
}
