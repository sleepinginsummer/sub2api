//go:build unit

package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 定时器按配置休息域寻找最近截止，实际冷却仍跨配置共享。
func TestGatewayPoolLocalTimerUsesConfigurationRestDeadline(t *testing.T) {
	account, clone, other := rotationAccount(1, 7), rotationAccount(2, 7), rotationAccount(3, 7)
	first, second := newGwpoolFakePool(t, "offline", 150), newGwpoolFakePool(t, "offline", 150)
	first.configure(account, clone)
	second.configure(other)
	for _, row := range []*Account{account, clone, other} {
		row.Extra[openAIGatewayPoolCooldownResetHoursKey] = 0
	}
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account, *clone, *other}}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	ctx := context.Background()
	identity, err := svc.codexCookies.gatewayPoolIdentity(ctx, account)
	require.NoError(t, err)
	now := time.Now().UTC()
	svc.codexCookies.poolUsed.Store(gatewayPoolLedgerKey(identity, "known"), now.Add(-time.Hour+12*time.Second))
	deadline := svc.codexCookies.gatewayPoolLocalResumeAt(identity, account)
	require.True(t, deadline.After(now))
	require.NoError(t, svc.enterGatewayPoolRest(ctx, account, identity, now, deadline))
	delay := svc.maintainGatewayPoolRests(ctx)
	require.Greater(t, delay, 5*time.Second)
	require.Less(t, delay, 20*time.Second, "配置域缓存必须让定时器睡到真实截止，不能退回一分钟轮询")
	allowed, err := svc.gatewayPoolResumeAllowed(ctx, clone, false)
	require.NoError(t, err)
	require.False(t, allowed)
	allowed, err = svc.gatewayPoolResumeAllowed(ctx, other, false)
	require.NoError(t, err)
	require.True(t, allowed, "共享冷却不能把另一个配置也标为休息")
	require.Zero(t, first.hits.Load())
	require.Zero(t, second.hits.Load())
}

// 新增本地恢复和显示逻辑必须读取共享消耗前缀，否则带配置的身份会漏掉真实触碰。
func TestGatewayPoolLocalDeadlineAndDisplayShareConsumptionAcrossConfigurations(t *testing.T) {
	account, other := gwpoolTestAccount(1), gwpoolTestAccount(2)
	first, second := newGwpoolFakePool(t, "offline", 150), newGwpoolFakePool(t, "offline", 150)
	first.configure(account)
	second.configure(other)
	svc := &OpenAIGatewayService{}
	identity, err := svc.codexCookies.gatewayPoolIdentity(context.Background(), account)
	require.NoError(t, err)
	otherIdentity, err := svc.codexCookies.gatewayPoolIdentity(context.Background(), other)
	require.NoError(t, err)
	require.NotEqual(t, gatewayPoolRestTag(identity), gatewayPoolRestTag(otherIdentity))
	now := time.Now().UTC()
	svc.codexCookies.poolUsed.Store(gatewayPoolLedgerKey(identity, "cleared"), now.Add(-time.Minute))
	svc.codexCookies.applyGatewayPoolCooldownClear(identity, now, 3600)
	touch := time.Now().UTC()
	svc.codexCookies.poolUsed.Store(gatewayPoolLedgerKey(identity, "recent"), touch)
	deadline := svc.codexCookies.gatewayPoolLocalResumeAt(identity, account)
	require.True(t, deadline.After(now))
	require.Equal(t, deadline, svc.codexCookies.gatewayPoolLocalResumeAt(otherIdentity, other))
	view, _ := svc.gatewayPoolDisplaySnapshot(other, otherIdentity, nil)
	require.Contains(t, view.Seen, "cleared", "清理后跨配置显示仍保留已知网关")
	require.True(t, view.Seen["cleared"].Cooldown.Cleared)
	require.Equal(t, touch, view.Seen["recent"].At)
	require.Zero(t, first.hits.Load())
	require.Zero(t, second.hits.Load())
}
