package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type gatewayPoolSnapshotUpstream struct {
	cookieRecordingUpstream
	body            string
	responseRequest *http.Request
}

func (u *gatewayPoolSnapshotUpstream) Do(req *http.Request, proxy string, id int64, concurrency int) (*http.Response, error) {
	resp, err := u.cookieRecordingUpstream.Do(req, proxy, id, concurrency)
	resp.Body = io.NopCloser(strings.NewReader(u.body))
	resp.Request = u.responseRequest
	return resp, err
}

func TestGatewayPoolRouteSnapshotSurvivesCacheChanges(t *testing.T) {
	for _, state := range []string{"expired", "rotated", "removed"} {
		t.Run(state, func(t *testing.T) {
			first := gwpoolTestPairCookie(t, "unified-142")
			pool := newGwpoolFakePool(t, first, 150)
			pool.forceCookie = gwpoolTestPairCookie(t, "unified-84")
			redirect := httptest.NewRequest(http.MethodPost, "https://chatgpt.com/redirected", nil)
			upstream := &gatewayPoolSnapshotUpstream{responseRequest: redirect}
			svc := &OpenAIGatewayService{httpUpstream: upstream}
			account := pool.account(1)
			account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = false // 本组只验证传输快照。
			svc.codexCookies.Store(account, gwpoolTestURL, codexCookieUpstreamResponse())
			send := func() *http.Response {
				req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
				require.NoError(t, err)
				resp, _, err := svc.doOpenAIUpstreamOnce(req, "", account)
				require.NoError(t, err)
				t.Cleanup(func() { _ = resp.Body.Close() })
				return resp
			}
			resp := send()
			require.Equal(t, redirect.URL, resp.Request.URL, "只附加快照，不能丢失传输返回的 Request 信息")
			require.NotSame(t, redirect, resp.Request)
			snapshot := openAIGatewayPoolRoutePairFromResponse(resp)
			require.NotNil(t, snapshot)
			require.Equal(t, first, *snapshot)
			result := &OpenAIForwardResult{Model: "gpt-6-astra", UpstreamHeaders: resp.Header, GatewayPoolRoutePair: snapshot, GatewayPoolApplied: openAIGatewayPoolAppliedFromResponse(resp)}
			cacheKey := openAIGatewayPoolCacheKey(account, gwpoolTestIdentity)
			svc.codexCookies.poolPairs.Store(cacheKey, openAIGatewayPoolPair{cookie: first, gateway: "unified-142", until: time.Now().Add(-time.Second)})
			switch state {
			case "rotated":
				rotated := send()
				require.Equal(t, "unified-84", openAICodexRouteGateway(*openAIGatewayPoolRoutePairFromResponse(rotated)))
			case "removed":
				svc.codexCookies.poolPairs.Delete(cacheKey)
			}
			routePair, fromPool, poolGateway, _ := svc.routePairInUse(account, result.UpstreamHeaders, result.GatewayPoolApplied)
			require.Equal(t, first, routePair)
			require.True(t, fromPool)
			require.Equal(t, "unified-142", poolGateway)
			// 校验最终 UsageLog，而非只验证快照取值函数。
			logs := &openAIRecordUsageLogRepoStub{inserted: true}
			billing := &openAIRecordUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
			recorder := newOpenAIRecordUsageServiceWithBillingRepoForTest(logs, billing, &openAIRecordUsageUserRepoStub{}, &openAIRecordUsageSubRepoStub{}, nil)
			require.NoError(t, recorder.RecordUsage(context.Background(), &OpenAIRecordUsageInput{
				Result: result, APIKey: &APIKey{ID: 1000, Quota: 100, Group: &Group{RateMultiplier: 1}}, User: &User{ID: 2000}, Account: account, APIKeyService: &openAIRecordUsageAPIKeyQuotaStub{},
			}))
			require.NotNil(t, logs.lastLog.RoutePair)
			require.Equal(t, first, *logs.lastLog.RoutePair)
			require.Equal(t, "unified-142", *logs.lastLog.RouteGateway)
			encoded, err := json.Marshal(result)
			require.NoError(t, err)
			require.NotContains(t, string(encoded), "GatewayPoolRoutePair", "内部快照不能进入对外 JSON")
		})
	}
}

func TestGatewayPoolRouteSnapshotForwardedToResults(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, passthrough := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("passthrough=%t/stream=%t", passthrough, stream), func(t *testing.T) {
				pair := gwpoolTestPairCookie(t, "unified-142")
				pool := newGwpoolFakePool(t, pair, 150)
				responseBody := `{"id":"resp_snapshot","object":"response","status":"completed","model":"gpt-6-astra","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`
				contentType := "application/json"
				if stream {
					contentType = "text/event-stream"
					responseBody = "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_snapshot\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\ndata: {\"type\":\"response.completed\",\"response\":" + responseBody + "}\n\n"
				}
				upstream := &gatewayPoolSnapshotUpstream{cookieRecordingUpstream: cookieRecordingUpstream{setCookie: http.Header{"Content-Type": []string{contentType}}}, body: responseBody}
				cfg := &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize, OpenAIFirstOutputTimeoutSeconds: 1}}
				svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream, responseHeaderFilter: compileResponseHeaderFilter(cfg)}
				account := pool.account(1)
				account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = false // 本组只验证出口快照及序列化。
				account.Credentials["access_token"] = "test-access-token"
				account.Extra["openai_passthrough"] = passthrough
				body := []byte(fmt.Sprintf(`{"model":"gpt-6-astra","input":"hi","instructions":"test","stream":%t}`, stream))
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(string(body)))
				// OAuth 普通透传会强制流式；compact 路径覆盖真实非流式出口。
				if passthrough && !stream {
					c.Request.URL.Path = "/v1/responses/compact"
				}
				result, err := svc.Forward(c.Request.Context(), c, account, body)
				require.NoError(t, err)
				if passthrough && !stream {
					// compact 已不属于网关池覆写范围，结果应明确没有池票快照。
					require.Nil(t, result.GatewayPoolRoutePair)
				} else {
					require.NotNil(t, result.GatewayPoolRoutePair)
					require.Equal(t, pair, *result.GatewayPoolRoutePair)
				}
				require.Equal(t, stream, result.Stream)
				require.NotContains(t, rec.Body.String(), pair, "Cookie 快照不能进入客户端响应")
			})
		}
	}
}

func TestGatewayPoolRouteSnapshotDisabledHasNoSnapshot(t *testing.T) {
	upstream := &cookieRecordingUpstream{}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolExtraKey] = false
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
	require.NoError(t, err)
	resp, err := svc.doOpenAIUpstream(req, "", account)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Nil(t, openAIGatewayPoolRoutePairFromResponse(resp))
}
