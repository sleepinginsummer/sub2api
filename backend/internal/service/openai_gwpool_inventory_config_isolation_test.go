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
	keyA := openAIGatewayPoolCacheKey(a, gwpoolTestIdentity)
	finish := store.gatewayPoolInventoryOperation(keyA)
	_, pendingA := store.gatewayPoolInventorySnapshot(gwpoolTestIdentity, a)
	generationB, pendingB := store.gatewayPoolInventorySnapshot(gwpoolTestIdentity, b)
	require.True(t, pendingA)
	require.False(t, pendingB, "其它池的在途取票不能挡住本池耗尽确认")
	finish()
	generationAfter, pendingB := store.gatewayPoolInventorySnapshot(gwpoolTestIdentity, b)
	require.False(t, pendingB)
	require.Equal(t, generationB, generationAfter, "其它池操作不能改变本池库存代数")

	pair := openAIGatewayPoolPair{cookie: "offline", gateway: "unified-142", version: "v", until: time.Now().Add(time.Minute * 3)}
	store.gatewayPoolSpareShelve(keyA, &gatewayPoolTicketBatch{store: store, account: a, identity: gwpoolTestIdentity, pairs: []openAIGatewayPoolPair{pair}})
	_, pendingA = store.gatewayPoolInventorySnapshot(gwpoolTestIdentity, a)
	_, pendingB = store.gatewayPoolInventorySnapshot(gwpoolTestIdentity, b)
	require.True(t, pendingA, "本池未尝试的库存仍可用")
	require.False(t, pendingB, "其它池不能借用本池库存")
	clone := *a
	clone.ID = 3
	_, pendingClone := store.gatewayPoolInventorySnapshot(gwpoolTestIdentity, &clone)
	require.True(t, pendingClone, "同凭据同池配置的克隆行仍共享库存")

	store.gatewayPoolMarkUsed(gwpoolTestIdentity, pair.gateway)
	_, pendingA = store.gatewayPoolInventorySnapshot(gwpoolTestIdentity, a)
	require.False(t, pendingA, "其它请求已经消耗的真实网关不能再当作未尝试库存")
	require.True(t, store.gatewayPoolUsedRecently(gwpoolTestIdentity, pair.gateway, b.gatewayPoolGatewayWindow()))
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
