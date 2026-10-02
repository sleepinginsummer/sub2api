package service

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// 同一转发上下文中的后续请求可以更新 sink，但不能改写已经发送请求的完整票据。
func TestGatewayPoolHTTPRouteSnapshotSurvivesSharedSinkOverwrite(t *testing.T) {
	pool := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	account := pool.account(1)
	svc := &OpenAIGatewayService{httpUpstream: &cookieRecordingUpstream{}}
	ctx, sink := withOpenAIGatewayPoolSink(context.Background())
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
	require.NoError(t, err)
	response, err := svc.doOpenAIUpstream(request, "", account)
	require.NoError(t, err)
	defer func() { require.NoError(t, response.Body.Close()) }()
	frozen := openAIGatewayPoolAppliedFromResponse(response)
	require.Equal(t, "tkt-1", frozen.Version)
	require.Equal(t, "unified-142", frozen.Gateway)

	// 模拟同账号的另一发在首发完成记账前取得不同路由和票号。
	sink.mark(OpenAIGatewayPoolApplied{
		AccountID: account.ID, Cookie: gwpoolTestPairCookie(t, "unified-84"),
		Gateway: "unified-84", Version: "later-ticket",
	})
	result := &OpenAIForwardResult{
		GatewayPoolRoutePair: openAIGatewayPoolRoutePairFromResponse(response),
		GatewayPoolApplied:   openAIGatewayPoolAppliedFromResponse(response),
	}
	sink.publish(result)
	require.Equal(t, frozen, result.GatewayPoolApplied)
	pair, overridden, gateway, version := svc.routePairInUse(account, response.Header, result.GatewayPoolApplied)
	require.Equal(t, frozen.Cookie, pair)
	require.True(t, overridden)
	require.Equal(t, frozen.Gateway, gateway)
	require.Equal(t, frozen.Version, version)

	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "GatewayPoolApplied")
	require.NotContains(t, string(encoded), frozen.Cookie)
}
