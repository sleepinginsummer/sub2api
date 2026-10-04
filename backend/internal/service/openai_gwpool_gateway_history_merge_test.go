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

func TestGatewayHistoryMergePreservesCooldownAndCredentialDomain(t *testing.T) {
	now := time.Now().UTC()
	cooldown := &gatewayPoolCooldown{UpdatedAt: now, WindowSeconds: 7200, Until: now.Add(2 * time.Hour)}
	existing := map[string]any{OpenAIGatewayHistoryExtraKey: openAIGatewayHistory{
		LedgerTag: "old-domain", Current: "unified-84", UpdatedAt: now,
		Seen: map[string]openAIGatewaySeen{"unified-84": {At: now, Cooldown: cooldown}},
	}}
	// 新请求没有冷却读数时，行锁合并必须保留先前学习结果。
	delta := map[string]any{OpenAIGatewayHistoryExtraKey: openAIGatewayHistory{
		LedgerTag: "old-domain", UpdatedAt: now.Add(time.Minute),
		Seen: map[string]openAIGatewaySeen{"unified-84": {At: now.Add(time.Minute)}},
	}}
	merged, err := MergeOpenAIGatewayHistoryExtra(existing, delta)
	require.NoError(t, err)
	rec, ok := readOpenAIGatewayHistory(&Account{Extra: merged})
	require.True(t, ok)
	require.Equal(t, cooldown, rec.Seen["unified-84"].Cooldown)
	require.Equal(t, "old-domain", merged[openAIGatewayLedgerTagExtraKey])

	// 凭证域切换时清掉旧域历史，顶层查询标签与嵌套账本始终一致。
	switched := map[string]any{OpenAIGatewayHistoryExtraKey: openAIGatewayHistory{
		LedgerTag: "new-domain", Current: "unified-142", UpdatedAt: now.Add(2 * time.Minute),
		Seen: map[string]openAIGatewaySeen{"unified-142": {At: now.Add(2 * time.Minute)}},
	}, openAIGatewayLedgerTagExtraKey: "new-domain"}
	merged, err = MergeOpenAIGatewayHistoryExtra(merged, switched)
	require.NoError(t, err)
	rec, ok = readOpenAIGatewayHistory(&Account{Extra: merged})
	require.True(t, ok)
	require.Equal(t, "new-domain", rec.LedgerTag)
	require.NotContains(t, rec.Seen, "unified-84")
	require.Equal(t, "unified-142", rec.Current)

	// 迟到的旧域写入不能覆盖新域及其索引标签。
	merged, err = MergeOpenAIGatewayHistoryExtra(merged, delta)
	require.NoError(t, err)
	rec, ok = readOpenAIGatewayHistory(&Account{Extra: merged})
	require.True(t, ok)
	require.Equal(t, "new-domain", rec.LedgerTag)
	require.Len(t, rec.Seen, 1)
	require.Equal(t, "new-domain", merged[openAIGatewayLedgerTagExtraKey])
}
