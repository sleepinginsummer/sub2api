package service

import (
	"sync"
	"time"
)

// 未实际验证的库存不能仅凭池端 UsedByYou 判为耗尽；代际用于发现并发取票或验证。
type gatewayPoolInventoryState struct {
	mu         sync.Mutex
	generation uint64
	active     int
	requests   int // 准备等待者只用于用量空闲检测。
}

func (s *openAICodexCookieStore) gatewayPoolInventory(identity string) *gatewayPoolInventoryState {
	value, _ := s.poolInventory.LoadOrStore(gatewayPoolLedgerIdentity(identity), &gatewayPoolInventoryState{})
	state, _ := value.(*gatewayPoolInventoryState)
	return state
}

func (s *openAICodexCookieStore) gatewayPoolInventoryOperation(identity string) func() {
	state := s.gatewayPoolInventory(identity)
	state.mu.Lock()
	state.active++
	state.generation++
	state.mu.Unlock()
	return func() {
		state.mu.Lock()
		state.active--
		state.generation++
		state.mu.Unlock()
	}
}

func (s *openAICodexCookieStore) gatewayPoolInventorySnapshot(identity string, account *Account) (generation uint64, pending bool) {
	generation, active, candidates := s.gatewayPoolInventoryCandidates(identity, account)
	return generation, active || len(candidates) > 0
}

// 库存遍历只归并同配置的身份克隆；实际消耗读数使用独立的上游账号作用域。
func (s *openAICodexCookieStore) gatewayPoolInventoryCandidates(identity string, account *Account) (generation uint64, active bool, candidates map[string]struct{}) {
	identity = openAIGatewayPoolCacheKey(account, identity)
	state := s.gatewayPoolInventory(identity)
	state.mu.Lock()
	defer state.mu.Unlock()
	candidates = map[string]struct{}{}
	if state.active > 0 {
		return state.generation, true, candidates
	}
	domain := gatewayPoolLedgerIdentity(identity)
	s.poolPairs.Range(func(key, _ any) bool {
		other, ok := key.(string)
		if ok && gatewayPoolLedgerIdentity(other) == domain {
			if pair, live := s.cachedPoolPair(other); live == openAIGatewayPoolPairLive {
				candidates[pair.gateway] = struct{}{}
			}
		}
		return true
	})
	s.poolSpare.Range(func(key, value any) bool {
		other, ok := key.(string)
		if !ok || gatewayPoolLedgerIdentity(other) != domain {
			return true
		}
		if batch, valid := value.(*gatewayPoolTicketBatch); valid {
			for _, pair := range batch.pairs[batch.idx:] {
				if pair.cookie == "" || pair.invalidated || pair.routeExpired(time.Now()) {
					continue
				}
				if _, cooling := s.gatewayPoolUsedAt(identity, pair.gateway, account.gatewayPoolGatewayWindow(), account.gatewayPoolUseRecommendation()); !cooling {
					candidates[pair.gateway] = struct{}{}
				}
			}
		}
		return true
	})
	return state.generation, false, candidates
}
