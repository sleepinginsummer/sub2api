//go:build unit

package service

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestGatewayHistoryMergePreservesOtherRequestsAndNewestFacts(t *testing.T) {
	now := time.Now().UTC()
	oldFree, newFree := 7, 0
	existing := map[string]any{OpenAIGatewayHistoryExtraKey: openAIGatewayHistory{
		Current: "unified-84", CurrentRegion: "us-east", PoolLive: 62, PoolFree: &oldFree, UpdatedAt: now,
		Seen: map[string]openAIGatewaySeen{"unified-84": {At: now, Verdict: "full", FullHeldMs: 60000}},
	}}
	older := map[string]any{OpenAIGatewayHistoryExtraKey: openAIGatewayHistory{
		Current: "unified-142", PoolLive: 50, PoolFree: &newFree, UpdatedAt: now.Add(-time.Minute),
		Seen: map[string]openAIGatewaySeen{"unified-142": {At: now.Add(-time.Minute)}, "unified-84": {At: now.Add(-time.Minute), Verdict: "degraded"}},
	}}
	merged, err := MergeOpenAIGatewayHistoryExtra(existing, older)
	require.NoError(t, err)
	rec, ok := readOpenAIGatewayHistory(&Account{Extra: merged})
	require.True(t, ok)
	require.Len(t, rec.Seen, 2)
	require.Equal(t, "unified-84", rec.Current)
	require.Equal(t, "full", rec.Seen["unified-84"].Verdict)
	require.Equal(t, int64(60000), rec.Seen["unified-84"].FullHeldMs)
	require.Equal(t, 62, rec.PoolLive)
	require.NotNil(t, rec.PoolFree)
	require.Equal(t, 7, *rec.PoolFree)
	// 无新清单、无推进当前网关的观测只补落点，不能清除已有测量或当前落点。
	delta := map[string]any{OpenAIGatewayHistoryExtraKey: openAIGatewayHistory{UpdatedAt: now.Add(time.Minute), Seen: map[string]openAIGatewaySeen{"unified-195": {At: now.Add(time.Minute)}}}}
	merged, err = MergeOpenAIGatewayHistoryExtra(merged, delta)
	require.NoError(t, err)
	rec, ok = readOpenAIGatewayHistory(&Account{Extra: merged})
	require.True(t, ok)
	require.Len(t, rec.Seen, 3)
	require.Equal(t, "unified-84", rec.Current)
	require.NotNil(t, rec.PoolFree)
	require.Equal(t, 7, *rec.PoolFree)
}
