package service

import (
	"encoding/json"
	"fmt"
)

// MergeOpenAIGatewayHistoryExtra 供仓储在账号行锁内合并增量，按成员标签保留当前和上一视图。
func MergeOpenAIGatewayHistoryExtra(existing, updates map[string]any) (map[string]any, error) {
	incoming, ok := readOpenAIGatewayHistory(&Account{Extra: updates})
	if !ok {
		return nil, fmt.Errorf("invalid gateway history update")
	}
	current, _ := readOpenAIGatewayHistory(&Account{Extra: existing})
	// 保持凭证域切换保护：迟到的旧域写入不能切回或污染当前视图。
	if incoming.LedgerTag != "" && current.LedgerTag != "" && incoming.LedgerTag != current.LedgerTag {
		if incoming.UpdatedAt.Before(current.UpdatedAt) {
			incoming = openAIGatewayHistory{}
		} else {
			current = gatewayPoolHistoryForTag(current, incoming.LedgerTag)
		}
	}
	mergeGatewayPoolHistoryView(&current, incoming)
	if incoming.Previous != nil {
		if current.Previous != nil && current.Previous.LedgerTag == incoming.Previous.LedgerTag {
			// 清理和重置上一成员时，根视图的观察时间可能没有变化，不能以它阻止合并。
			mergeGatewayPoolHistoryView(current.Previous, *incoming.Previous)
		} else if current.Previous == nil && !incoming.UpdatedAt.Before(current.UpdatedAt) {
			previous := *incoming.Previous
			previous.Previous = nil
			current.Previous = &previous
		}
	}
	if current.Previous != nil {
		current.Previous.Previous = nil
	}
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
	previousTag := ""
	if current.Previous != nil {
		previousTag = current.Previous.LedgerTag
	}
	result[openAIGatewayPreviousLedgerTagExtraKey] = previousTag
	return result, nil
}

// 每个成员视图独立比较观察时间及冷却代际，禁止递归带入第三层历史。
func mergeGatewayPoolHistoryView(current *openAIGatewayHistory, incoming openAIGatewayHistory) {
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
			// 清理屏障可以晚于旧观察，保留新观察的同时接纳更新的冷却代际。
			if newerGatewayPoolCooldown(next.Cooldown, previous.Cooldown) {
				previous.Cooldown = next.Cooldown
				current.Seen[gateway] = previous
			}
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
		if next.Cooldown == nil || newerGatewayPoolCooldown(previous.Cooldown, next.Cooldown) {
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
	reset := current.CooldownReset
	if !incoming.UpdatedAt.Before(current.UpdatedAt) {
		reset.IntervalHours, reset.StartedAt = incoming.CooldownReset.IntervalHours, incoming.CooldownReset.StartedAt
	}
	if incoming.CooldownReset.LastAt.After(reset.LastAt) {
		reset.LastAt = incoming.CooldownReset.LastAt
	}
	if incoming.CooldownReset.ClearedAt.After(reset.ClearedAt) {
		reset.ClearedAt = incoming.CooldownReset.ClearedAt
	}
	current.CooldownReset = reset
	if incoming.UpdatedAt.After(current.UpdatedAt) {
		current.UpdatedAt = incoming.UpdatedAt
	}
	pruneOpenAIGatewayHistory(current)
}
