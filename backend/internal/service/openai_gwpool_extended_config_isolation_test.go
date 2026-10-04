package service

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 备用票必须留在交付它的池配置里；相同凭证身份切池后不能弹出旧池的票。
func TestGatewayPoolSpareShelfSeparatesAccountConfigurations(t *testing.T) {
	poolA := newGwpoolFakePool(t, "", 150)
	poolA.batchGateways = []string{"unified-11", "unified-22", "unified-33"}
	poolB := newGwpoolFakePool(t, "", 150)
	poolB.batchGateways = []string{"unified-44", "unified-55", "unified-66"}
	store := &openAICodexCookieStore{}
	a, b := poolA.account(1), poolB.account(2)
	require.NoError(t, attachRoute(context.Background(), store, a, gwpoolTestURL, http.Header{}))

	headers := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, b, gwpoolTestURL, headers))
	require.Equal(t, "unified-44", openAICodexRouteGateway(headers.Get("Cookie")))
	require.EqualValues(t, 1, poolB.hits.Load(), "切池必须向对应池取票")

	keyA := openAIGatewayPoolCacheKey(a, gwpoolTestIdentity)
	pair, _ := store.cachedPoolPair(keyA)
	store.gatewayPoolMarkStale(keyA, pair.version, pair.gateway)
	rotated := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, a, gwpoolTestURL, rotated))
	require.Equal(t, "unified-22", openAICodexRouteGateway(rotated.Get("Cookie")))
	require.EqualValues(t, 1, poolA.hits.Load(), "其它配置不能取走本池的备用票")
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

	// 未验的新配置不能借旧配置的快路跳过模型检查。
	b.Extra[openAIGatewayPoolProbeModelExtraKey] = gatewayPoolProbeModelBusiness
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("not json"))
	require.NoError(t, err)
	require.ErrorIs(t, svc.gatewayPoolWarmUp(req, "", b), errOpenAIGatewayPoolWarmNoModel)

	second := &gwpoolWarmShooter{}
	require.NoError(t, gwpoolWarmRun(t, svc, b, second))
	require.Len(t, second.shots, 2, "相同票号也必须验本池交付的票")
	require.Equal(t, "unified-84", openAICodexRouteGateway(second.shots[0].cookie))
	require.EqualValues(t, 1, poolB.hits.Load())
}

// 后台预热的 CAS 只能替换本配置的当前票，不能更新同身份其它池的缓存。
func TestGatewayPoolPrewarmSwapSeparatesAccountConfigurations(t *testing.T) {
	poolA := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	poolB := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-84"), 150)
	svc := &OpenAIGatewayService{}
	a, b := poolA.account(1), poolB.account(2)
	keyA := openAIGatewayPoolCacheKey(a, gwpoolTestIdentity)
	keyB := openAIGatewayPoolCacheKey(b, gwpoolTestIdentity)
	currentA := gwpoolSeedVerifiedAge(&svc.codexCookies, keyA, "tkt-old", 190*time.Second)
	currentB := gwpoolSeedVerifiedAge(&svc.codexCookies, keyB, "tkt-old", 190*time.Second)
	shooter := &gwpoolWarmShooter{}
	svc.gatewayPoolPrewarmRound(context.Background(), b, gwpoolTestIdentity,
		currentB, 190*time.Second, shooter.shoot)

	unchanged, _ := svc.codexCookies.cachedPoolPair(keyA)
	require.Equal(t, currentA, unchanged)
	updated, state := svc.codexCookies.cachedPoolPair(keyB)
	require.Equal(t, openAIGatewayPoolPairLive, state)
	require.Equal(t, "unified-84", updated.gateway)
	require.True(t, svc.codexCookies.gatewayPoolVerifiedFull(keyB))
	require.Len(t, shooter.shots, 2)
}
