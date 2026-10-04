package service

import (
	"encoding/json"
	"fmt"
)

// MergeOpenAIGatewayHistoryExtra 供仓储在账号行锁内使用：按落点时间合并增量，
// 保留其它请求的记录和未知读数，最后仍按既有上限裁剪。
func MergeOpenAIGatewayHistoryExtra(existing, updates map[string]any) (map[string]any, error) {
	incoming, ok := readOpenAIGatewayHistory(&Account{Extra: updates})
	if !ok {
		return nil, fmt.Errorf("invalid gateway history update")
	}
	current, _ := readOpenAIGatewayHistory(&Account{Extra: existing})
	// 冷却账本随凭证域切换，旧域的落点不能污染新域；迟到的旧域写入也不能倒退。
	if incoming.LedgerTag != "" && current.LedgerTag != "" && incoming.LedgerTag != current.LedgerTag {
		if incoming.UpdatedAt.Before(current.UpdatedAt) {
			incoming = openAIGatewayHistory{}
		} else {
			current = openAIGatewayHistory{}
		}
	}
	if incoming.LedgerTag != "" {
		current.LedgerTag = incoming.LedgerTag
	}
	currentAt := current.Seen[current.Current].At
	incomingAt := incoming.Seen[incoming.Current].At
	if current.Seen == nil {
		current.Seen = make(map[string]openAIGatewaySeen)
	}
	for gateway, next := range incoming.Seen {
		previous, exists := current.Seen[gateway]
		if exists && previous.At.After(next.At) {
			continue
		}
		if next.Region == "" {
			next.Region = previous.Region
		}
		if next.Verdict == "" {
			next.Verdict = previous.Verdict
		}
		if previous.FullAt.After(next.FullAt) {
			next.FullAt = previous.FullAt
		}
		if next.FullHeldMs <= 0 {
			next.FullHeldMs = previous.FullHeldMs
		}
		// 未测到冷却或旧观察不能擦掉后续学到的冷却参数。
		if next.Cooldown == nil || (previous.Cooldown != nil && previous.Cooldown.UpdatedAt.After(next.Cooldown.UpdatedAt)) {
			next.Cooldown = previous.Cooldown
		}
		current.Seen[gateway] = next
	}
	if incoming.Current != "" && !incomingAt.Before(currentAt) {
		current.Current, current.CurrentRegion = incoming.Current, incoming.CurrentRegion
		if current.CurrentRegion == "" {
			current.CurrentRegion = current.Seen[incoming.Current].Region
		}
	}
	if incoming.PoolLive > 0 && incoming.PoolFree != nil && !incoming.UpdatedAt.Before(current.UpdatedAt) {
		current.PoolLive, current.PoolFree = incoming.PoolLive, incoming.PoolFree
	}
	if incoming.UpdatedAt.After(current.UpdatedAt) {
		current.UpdatedAt = incoming.UpdatedAt
	}
	pruneOpenAIGatewayHistory(&current)
	encoded, err := json.Marshal(current)
	if err != nil {
		return nil, err
	}
	var merged map[string]any
	if err := json.Unmarshal(encoded, &merged); err != nil {
		return nil, err
	}
	result := make(map[string]any, len(updates))
	for key, value := range updates {
		result[key] = value
	}
	result[OpenAIGatewayHistoryExtraKey] = merged
	if current.LedgerTag != "" {
		result[openAIGatewayLedgerTagExtraKey] = current.LedgerTag
	}
	return result, nil
}
