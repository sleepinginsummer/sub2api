//go:build unit

package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 休息是配置下的供给状态；冷却是同一上游账号的实际消耗，二者不能共用隔离边界。
func TestGatewayPoolRestSeparatesConfigurationsAndSharesCooling(t *testing.T) {
	account, clone, other := gwpoolTestAccount(1), gwpoolTestAccount(2), gwpoolTestAccount(3)
	first := newGwpoolFakePool(t, "offline", 150)
	second := newGwpoolFakePool(t, "offline", 150)
	first.configure(account, clone)
	second.configure(other)
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account, *clone, *other}}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	ctx := context.Background()
	identity, err := svc.codexCookies.gatewayPoolIdentity(ctx, account)
	require.NoError(t, err)
	otherIdentity, err := svc.codexCookies.gatewayPoolIdentity(ctx, other)
	require.NoError(t, err)
	require.NotEqual(t, gatewayPoolRestTag(identity), gatewayPoolRestTag(otherIdentity))
	require.Equal(t, gatewayPoolLedgerTag(identity), gatewayPoolLedgerTag(otherIdentity))
	now := time.Now().UTC()
	require.NoError(t, svc.enterGatewayPoolRest(ctx, account, identity, now, now.Add(time.Minute)))
	allowed, err := svc.gatewayPoolResumeAllowed(ctx, clone, false)
	require.NoError(t, err)
	require.False(t, allowed, "同配置克隆应继承已持久化的休息状态")
	allowed, err = svc.gatewayPoolResumeAllowed(ctx, other, false)
	require.NoError(t, err)
	require.True(t, allowed, "另一个池配置不能继承供给不足状态")
	svc.codexCookies.gatewayPoolMarkUsed(identity, "unified-142")
	require.True(t, svc.codexCookies.gatewayPoolUsedRecently(otherIdentity, "unified-142", time.Hour))
	require.Zero(t, first.hits.Load())
	require.Zero(t, second.hits.Load())
}

// 行锁内合并既要保存成员切换前的账本，也不能让迟到的观测回滚清冷却屏障。
func TestGatewayHistoryMergeKeepsPreviousViewAndResetBarriers(t *testing.T) {
	now := time.Now().UTC()
	old := openAIGatewayHistory{LedgerTag: "old", UpdatedAt: now,
		Seen:          map[string]openAIGatewaySeen{"old-gateway": {At: now}},
		CooldownReset: gatewayPoolCooldownResetState{IntervalHours: 24, LastAt: now, ClearedAt: now}}
	patch := openAIGatewayHistory{LedgerTag: "new", UpdatedAt: now.Add(time.Minute),
		Seen: map[string]openAIGatewaySeen{"new-gateway": {At: now.Add(time.Minute)}}}
	merged, err := MergeOpenAIGatewayHistoryExtra(
		map[string]any{OpenAIGatewayHistoryExtraKey: old},
		map[string]any{OpenAIGatewayHistoryExtraKey: patch})
	require.NoError(t, err)
	history, ok := readOpenAIGatewayHistory(&Account{Extra: merged})
	require.True(t, ok)
	require.NotNil(t, history.Previous)
	require.Equal(t, "old", history.Previous.LedgerTag)
	require.Contains(t, history.Previous.Seen, "old-gateway")
	require.Equal(t, "old", merged[openAIGatewayPreviousLedgerTagExtraKey])
	// 回到旧成员时恢复该成员视图；上一成员视图仍只保留一层。
	patch.LedgerTag, patch.UpdatedAt = "old", now.Add(2*time.Minute)
	patch.CooldownReset = old.CooldownReset
	merged, err = MergeOpenAIGatewayHistoryExtra(merged, map[string]any{OpenAIGatewayHistoryExtraKey: patch})
	require.NoError(t, err)
	late := openAIGatewayHistory{LedgerTag: "old", UpdatedAt: now.Add(-time.Minute),
		CooldownReset: gatewayPoolCooldownResetState{LastAt: now.Add(-time.Hour)}}
	merged, err = MergeOpenAIGatewayHistoryExtra(merged, map[string]any{OpenAIGatewayHistoryExtraKey: late})
	require.NoError(t, err)
	history, ok = readOpenAIGatewayHistory(&Account{Extra: merged})
	require.True(t, ok)
	require.Contains(t, history.Seen, "old-gateway")
	require.Equal(t, now, history.CooldownReset.LastAt)
	require.Equal(t, now, history.CooldownReset.ClearedAt)
	require.Equal(t, 24, history.CooldownReset.IntervalHours)
	require.Equal(t, "new", history.Previous.LedgerTag)
	require.Nil(t, history.Previous.Previous)
}
