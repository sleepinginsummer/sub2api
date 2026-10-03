//go:build unit

package service

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGatewayPoolLegacyCountsRemainUnmeasured(t *testing.T) {
	repo := newTurnStateAutoRepo()
	svc := &OpenAIGatewayService{accountRepo: repo}
	old := map[string]any{"current": "unified-142", "pool_live": 62, "seen": map[string]any{}}
	account := &Account{ID: 7, Extra: map[string]any{openAIGatewayHistoryExtraKey: old}}
	require.NotContains(t, old, "pool_free")
	// 升级前已有 live、没有 free；本次只刷新落点，没有取到新清单。
	svc.noteOpenAIGatewayUse(context.Background(), account, "unified-84", "us-east", "full", true, 0, 0, 0)
	require.Len(t, repo.extraWrites, 1)
	history := account.Extra[openAIGatewayHistoryExtraKey].(map[string]any)
	live, _ := history["pool_live"].(float64)
	free, hasFree := history["pool_free"].(float64)
	t.Logf("rewritten live=%v free=%v free-present=%v", live, free, hasFree)
	require.False(t, live > 0 && hasFree, "没有测到剩余数，不能改写成前端认定已测得 0 的记录")
}
