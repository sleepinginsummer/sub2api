package service

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// 单票轮换只能替换原池配置的缓存，不得触碰其它池的同号票。
func TestGatewayPoolTicketRotationSeparatesAccountConfigurations(t *testing.T) {
	poolA := newGwpoolFakePool(t, "", 150)
	poolA.cookieForHit = func(hit int64) string {
		if hit == 1 {
			return gwpoolTestPairCookie(t, "unified-11")
		}
		return gwpoolTestPairCookie(t, "unified-22")
	}
	poolB := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-44"), 150)
	store := &openAICodexCookieStore{}
	a, b := poolA.account(1), poolB.account(2)
	require.NoError(t, attachRoute(context.Background(), store, a, gwpoolTestURL, http.Header{}))
	require.NoError(t, attachRoute(context.Background(), store, b, gwpoolTestURL, http.Header{}))
	keyA, keyB := openAIGatewayPoolAccountKey(a), openAIGatewayPoolAccountKey(b)
	pair, _ := store.cachedPoolPair(keyA)
	other, _ := store.cachedPoolPair(keyB)
	store.gatewayPoolMarkStale(keyA, pair.version)
	headers := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, a, gwpoolTestURL, headers))
	require.Equal(t, "unified-22", openAICodexRouteGateway(headers.Get("Cookie")))
	require.EqualValues(t, 2, poolA.hits.Load())
	unchanged, _ := store.cachedPoolPair(keyB)
	require.Equal(t, other, unchanged)
	require.EqualValues(t, 1, poolB.hits.Load())
}

// 同票号不意味着同一池票；验满血快路和探测结果必须随池配置隔离。
func TestGatewayPoolWarmVerificationSeparatesAccountConfigurations(t *testing.T) {
	poolA := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	poolB := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-84"), 150)
	svc := &OpenAIGatewayService{}
	a, b := poolA.account(1), poolB.account(2)
	first := &gwpoolWarmShooter{}
	require.NoError(t, gwpoolWarmRun(t, svc, a, first))
	require.Len(t, first.shots, 2)

	second := &gwpoolWarmShooter{}
	require.NoError(t, gwpoolWarmRun(t, svc, b, second))
	require.Len(t, second.shots, 2, "相同票号也必须验本池交付的票")
	require.Equal(t, "unified-84", openAICodexRouteGateway(second.shots[0].cookie))
	require.EqualValues(t, 1, poolB.hits.Load())
}

// 新准备流程只能替换本配置的票，不能改写另一配置的已验证快路。
func TestGatewayPoolPreparationSwapSeparatesAccountConfigurations(t *testing.T) {
	poolA := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	poolB := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-84"), 150)
	svc := &OpenAIGatewayService{}
	a, b := poolA.account(1), poolB.account(2)
	keyA := openAIGatewayPoolCacheKey(a, gwpoolTestIdentity)
	keyB := openAIGatewayPoolCacheKey(b, gwpoolTestIdentity)
	first := &gwpoolWarmShooter{}
	require.NoError(t, gwpoolWarmRun(t, svc, a, first))
	currentA, _ := svc.codexCookies.cachedPoolPair(keyA)
	shooter := &gwpoolWarmShooter{}
	require.NoError(t, gwpoolWarmRun(t, svc, b, shooter))

	unchanged, _ := svc.codexCookies.cachedPoolPair(keyA)
	require.Equal(t, currentA, unchanged)
	updated, state := svc.codexCookies.cachedPoolPair(keyB)
	require.Equal(t, openAIGatewayPoolPairLive, state)
	require.Equal(t, "unified-84", updated.gateway)
	require.True(t, svc.codexCookies.gatewayPoolVerifiedFull(keyB))
	require.Len(t, shooter.shots, 2)
}
