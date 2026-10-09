//go:build unit

package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 在途取票和库存必须按池配置隔离，冷却消耗仍按真实上游账号共享。
func TestGatewayPoolInventorySeparatesConfigurationsAndSharesConsumption(t *testing.T) {
	a, b := gwpoolTestAccount(1), gwpoolTestAccount(2)
	a.Extra[openAIGatewayPoolBaseURLExtraKey] = "http://127.0.0.1:1"
	b.Extra[openAIGatewayPoolBaseURLExtraKey] = "http://127.0.0.1:2"
	a.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "key-a"
	b.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "key-b"
	store := &openAICodexCookieStore{}
	keyA, keyB := openAIGatewayPoolCacheKey(a, gwpoolTestIdentity), openAIGatewayPoolCacheKey(b, gwpoolTestIdentity)
	finish := store.gatewayPoolInventoryOperation(keyA)
	_, pendingA := store.gatewayPoolInventorySnapshot(keyA)
	generationB, pendingB := store.gatewayPoolInventorySnapshot(keyB)
	require.True(t, pendingA)
	require.False(t, pendingB, "其它池的在途取票不能挡住本池耗尽确认")
	finish()
	generationAfter, pendingB := store.gatewayPoolInventorySnapshot(keyB)
	require.False(t, pendingB)
	require.Equal(t, generationB, generationAfter)
	pair := openAIGatewayPoolPair{cookie: "offline", gateway: "unified-142", version: "v", until: time.Now().Add(3 * time.Minute)}
	store.poolPairs.Store(keyA, pair)
	_, pendingA = store.gatewayPoolInventorySnapshot(keyA)
	_, pendingB = store.gatewayPoolInventorySnapshot(keyB)
	require.True(t, pendingA, "本配置的活票仍可复用")
	require.False(t, pendingB, "其它配置不能借用本池的票")
	clone := *a
	clone.ID = 3
	_, pendingClone := store.gatewayPoolInventorySnapshot(openAIGatewayPoolCacheKey(&clone, gwpoolTestIdentity))
	require.True(t, pendingClone, "同配置克隆共享当前票")
	store.gatewayPoolMarkUsed(keyA, pair.gateway)
	store.gatewayPoolMarkStale(keyA, pair.version)
	_, pendingA = store.gatewayPoolInventorySnapshot(keyA)
	require.False(t, pendingA, "已失效票不能继续参与耗尽判定")
	require.True(t, store.gatewayPoolUsedRecently(keyB, pair.gateway, b.gatewayPoolGatewayWindow()))
}

// 同凭据配置不同池时，耗尽判定不能让另一池退出调度；同配置克隆仍共享轮次。
func TestGatewayPoolRoundEligibilitySeparatesAccountConfigurations(t *testing.T) {
	const group int64 = 7
	a, b := rotationAccount(1, group), rotationAccount(2, group)
	a.Extra[openAIGatewayPoolBaseURLExtraKey] = "http://127.0.0.1:1"
	b.Extra[openAIGatewayPoolBaseURLExtraKey] = "http://127.0.0.1:2"
	a.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "key-a"
	b.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "key-b"
	svc := &OpenAIGatewayService{}
	keyA, keyB := openAIGatewayPoolCacheKey(a, gwpoolTestIdentity), openAIGatewayPoolCacheKey(b, gwpoolTestIdentity)
	svc.codexCookies.poolRounds.exhaust(group, keyA, 0)
	groupID := group
	require.False(t, svc.gatewayPoolRoundSelectionAllowed(context.Background(), &groupID, &AccountSelectionResult{Account: a}))
	require.True(t, svc.gatewayPoolRoundSelectionAllowed(context.Background(), &groupID, &AccountSelectionResult{Account: b}))
	clone := *a
	clone.ID = 3
	require.False(t, svc.gatewayPoolRoundSelectionAllowed(context.Background(), &groupID, &AccountSelectionResult{Account: &clone}))
	excluded, restarted := svc.codexCookies.poolRounds.snapshot(group, map[int64]string{
		a.ID: gatewayPoolLedgerIdentity(keyA), b.ID: gatewayPoolLedgerIdentity(keyB), clone.ID: gatewayPoolLedgerIdentity(keyA),
	}, true)
	require.False(t, restarted, "另一池未耗尽时不能重启全组轮次")
	require.Equal(t, map[int64]struct{}{a.ID: {}, clone.ID: {}}, excluded)
}
