//go:build unit

package service

import (
	"context"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/stretchr/testify/require"
)

func TestResolveUsageBillingRequestID_ForcedWebSearchBeatsClientID(t *testing.T) {
	t.Parallel()
	ctx := context.WithValue(context.Background(), ctxkey.ClientRequestID, "client-shared-id")
	got := resolveUsageBillingRequestID(ctx, "web_search:uuid-1")
	require.Equal(t, "web_search:uuid-1", got)
}

func TestResolveUsageBillingRequestID_ClientWinsOverPlainUpstream(t *testing.T) {
	t.Parallel()
	ctx := context.WithValue(context.Background(), ctxkey.ClientRequestID, "client-shared-id")
	got := resolveUsageBillingRequestID(ctx, "resp_abc")
	require.Equal(t, "client:client-shared-id", got)
}

func TestIsForcedUsageBillingRequestID(t *testing.T) {
	t.Parallel()
	require.True(t, isForcedUsageBillingRequestID("web_search:x"))
	require.True(t, isForcedUsageBillingRequestID("grok-video:task-1"))
	require.True(t, isForcedUsageBillingRequestID("grok_audio:up-1"))
	require.True(t, isForcedUsageBillingRequestID("grok_realtime:sess-1"))
	require.True(t, isForcedUsageBillingRequestID("gwpool_degraded:client:c-1:0"))
	require.True(t, isForcedUsageBillingRequestID(GeminiCachedContentCreateUsageRequestID("abc")))
	require.False(t, isForcedUsageBillingRequestID("resp_abc"))
}

// 网关池降智丢弃行和重试成功行必须使用不同计费幂等键。
func TestResolveUsageBillingRequestID_GwpoolDegradedNeverCollidesWithSuccess(t *testing.T) {
	t.Parallel()
	ctx := context.WithValue(context.Background(), ctxkey.ClientRequestID, "c-1")
	success := resolveUsageBillingRequestID(ctx, "resp_upstream")
	require.Equal(t, "client:c-1", success)

	base := "gwpool_degraded:" + resolveUsageBillingRequestID(ctx, "")
	first := resolveUsageBillingRequestID(ctx, base+":0")
	second := resolveUsageBillingRequestID(ctx, base+":1")
	require.Equal(t, base+":0", first, "丢弃行的键被 ctx 的 client id 盖掉了")
	require.NotEqual(t, success, first)
	require.NotEqual(t, first, second)
	require.Contains(t, first, "c-1")
}

func TestGeminiCachedContentUsageRequestIDs(t *testing.T) {
	t.Parallel()
	publicID, err := NewGeminiCachedContentPublicID()
	require.NoError(t, err)
	create := GeminiCachedContentCreateUsageRequestID(publicID)
	patch := GeminiCachedContentPatchUsageRequestID(publicID)
	require.Equal(t, "gcache:create:"+publicID, create)
	require.True(t, strings.HasPrefix(patch, "gcache:patch:"+publicID+":"))
	require.NotEqual(t, patch, GeminiCachedContentPatchUsageRequestID(publicID), "每次延长各自唯一")
	require.LessOrEqual(t, len(create), 64, "usage_logs.request_id 列长 64")
	require.LessOrEqual(t, len(patch), 64, "usage_logs.request_id 列长 64")

	ctx := context.WithValue(context.Background(), ctxkey.ClientRequestID, "client-shared-id")
	require.Equal(t, create, resolveUsageBillingRequestID(ctx, create), "缓存费用行不被客户端请求 id 覆盖")
	require.Equal(t, patch, resolveUsageBillingRequestID(ctx, patch))
}

func TestStableGrokAudioBillingRequestID(t *testing.T) {
	t.Parallel()
	require.Equal(t, "grok_audio:up-1", StableGrokAudioBillingRequestID("up-1"))
	require.Equal(t, "grok_audio:up-1", StableGrokAudioBillingRequestID("grok_audio:up-1"))
	got := StableGrokAudioBillingRequestID("")
	require.True(t, strings.HasPrefix(got, "grok_audio:"))
	require.Greater(t, len(got), len("grok_audio:"))
}

func TestStableGrokRealtimeBillingRequestID(t *testing.T) {
	t.Parallel()
	require.Equal(t, "grok_realtime:s1", StableGrokRealtimeBillingRequestID("s1"))
	require.Equal(t, "grok_realtime:s1", StableGrokRealtimeBillingRequestID("grok_realtime:s1"))
	got := StableGrokRealtimeBillingRequestID("")
	require.True(t, strings.HasPrefix(got, "grok_realtime:"))
}

func TestResolveUsageBillingRequestID_ForcedGrokAudioBeatsClientID(t *testing.T) {
	t.Parallel()
	ctx := context.WithValue(context.Background(), ctxkey.ClientRequestID, "client-shared-id")
	got := resolveUsageBillingRequestID(ctx, StableGrokAudioBillingRequestID("up-9"))
	require.Equal(t, "grok_audio:up-9", got)
}
