//go:build unit

package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// 两边从真实取票路径装配缓存，作用域键从缓存读取，兼容上游原键和本地配置隔离键。
func reviewPoolCacheKey(t *testing.T, store *openAICodexCookieStore) string {
	t.Helper()
	var key string
	count := 0
	store.poolPairs.Range(func(k, v any) bool { key = k.(string); count++; return true })
	require.Equal(t, 1, count)
	return key
}

func TestGatewayPoolFullHeldMeasurementReachesDiscardedAttempt(t *testing.T) {
	pool := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	account := pool.account(1)
	svc := &OpenAIGatewayService{}
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx, sink := withOpenAIGatewayPoolSink(context.Background(), ginCtx)
	_, err := svc.codexCookies.AttachRoute(ctx, account, gwpoolTestURL, http.Header{})
	require.NoError(t, err)
	applied := sink.snapshot()
	key := reviewPoolCacheKey(t, &svc.codexCookies)
	svc.codexCookies.poolVerified.Store(key, gatewayPoolVerifiedMark{version: applied.Version, at: time.Now().Add(-time.Minute)})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
	require.NoError(t, err)
	response := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}}
	svc.dropDegradedGatewayPoolRoute(request, response, account)
	measured := sink.snapshot().FullHeldMs
	require.GreaterOrEqual(t, measured, int64(60000), "确实量到了同票据满血窗口")
	attempts := takeDiscardedOpenAIGatewayPoolAttempts(ginCtx)
	require.Len(t, attempts, 1)
	require.Equal(t, measured, attempts[0].Applied.FullHeldMs, "用量侧落库使用的快照必须包含刚量到的时长")
}

func TestGatewayPoolDegradedUsesFrozenHTTPRoute(t *testing.T) {
	pool := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	account := pool.account(1)
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{{status: http.StatusOK, minted: gwpoolEchoFreshTicket}}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx, sink := withOpenAIGatewayPoolSink(context.Background(), ginCtx)
	_, err := svc.codexCookies.AttachRoute(ctx, account, gwpoolTestURL, http.Header{})
	require.NoError(t, err)
	key := reviewPoolCacheKey(t, &svc.codexCookies)
	first, _ := svc.codexCookies.cachedPoolPair(key)
	first.since = time.Now().Add(-3 * time.Minute)
	first.firstSent = first.since
	svc.codexCookies.poolPairs.Store(key, first)
	svc.codexCookies.poolVerified.Store(key, gatewayPoolVerifiedMark{version: first.version, at: time.Now().Add(-time.Minute)})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
	require.NoError(t, err)
	request.Header.Set(openAICodexTurnStateHeader, gwpoolEchoLiveTicket)
	response, degraded, err := svc.doOpenAIUpstreamOnce(request, "", account)
	require.NoError(t, err)
	require.True(t, degraded)
	frozen := openAIGatewayPoolAppliedFromResponse(response)
	require.Equal(t, "unified-142", frozen.Gateway)
	// 在首发降智处理前，模拟同一转发上下文另一发切换到已验满血的新票。
	second := openAIGatewayPoolPair{cookie: gwpoolTestPairCookie(t, "unified-84"), gateway: "unified-84", version: "next-ticket", since: time.Now(), until: time.Now().Add(time.Minute)}
	svc.codexCookies.poolPairs.Store(key, second)
	sink.mark(OpenAIGatewayPoolApplied{AccountID: account.ID, Cookie: second.cookie, Gateway: second.gateway, Version: second.version})
	svc.dropDegradedGatewayPoolRoute(request, response, account)
	attempts := takeDiscardedOpenAIGatewayPoolAttempts(ginCtx)
	require.Len(t, attempts, 1)
	_, stateBeforeAssertion := svc.codexCookies.cachedPoolPair(key)
	t.Logf("frozen=%s, discarded=%s, next-pair-state=%v", frozen.Gateway, attempts[0].Applied.Gateway, stateBeforeAssertion)
	require.Equal(t, frozen.Gateway, attempts[0].Applied.Gateway, "丢弃审计记录必须仍指向首发实际落点")
	_, state := svc.codexCookies.cachedPoolPair(key)
	require.Equal(t, openAIGatewayPoolPairLive, state, "另一发的新票不能被旧响应标成降智")
}
