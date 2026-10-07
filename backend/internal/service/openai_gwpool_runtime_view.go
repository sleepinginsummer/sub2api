package service

import (
	"context"
	"sort"
	"time"
)

type GatewayPoolLiveTicket struct {
	Gateway        string    `json:"gateway"`
	Region         string    `json:"region"`
	ExpiresAt      time.Time `json:"expires_at,omitzero"`
	VerifiedAt     time.Time `json:"verified_at,omitzero"`
	VerifiedModels []string  `json:"verified_models"`
}

type GatewayPoolRuntimeView struct {
	ObservedAt           time.Time                          `json:"observed_at"`
	Tickets              []GatewayPoolLiveTicket            `json:"tickets"`
	Rounds               []GatewayPoolUsageRound            `json:"rounds"`
	Archived             map[string]GatewayPoolUsageArchive `json:"archived"`
	Incomplete           bool                               `json:"incomplete,omitempty"`
	History              openAIGatewayHistory               `json:"history"`
	Contacts             gatewayPoolContacts                `json:"contacts"`
	LedgerTag            string                             `json:"ledger_tag"`
	GatewayWindowSeconds int                                `json:"gateway_window_seconds"`
	CurrentConcurrency   *int                               `json:"current_concurrency"`
	ConcurrencyLimit     int                                `json:"concurrency_limit"`
	CooldownEstimate     GatewayPoolCooldownEstimate        `json:"cooldown_estimate"`
	Rest                 GatewayPoolRestView                `json:"rest"`
}

// Read-only local snapshot. This endpoint never fetches tickets, lists gateways,
// ends a round, or probes an upstream model.
func (s *OpenAIGatewayService) GatewayPoolRuntimeProgress(ctx context.Context, ids []int64) (map[int64]GatewayPoolProgress, error) {
	if s.accountRepo == nil {
		return s.GatewayPoolProgress(ids), nil
	}
	out := map[int64]GatewayPoolProgress{}
	accounts, err := s.accountRepo.GetByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	var concurrency map[int64]int
	if s.concurrencyService != nil {
		// A failed/missing result stays nil. It must not render as measured zero.
		concurrency, err = s.concurrencyService.GetAccountConcurrencyBatch(ctx, ids)
		if err != nil {
			concurrency = nil
		}
	}
	historyPeers := map[string][]Account{}
	restPeers := map[string][]Account{}
	usageSnapshots := map[string][]Account{}
	usageLegacy := map[string]bool{}
	displayCache := gatewayPoolDisplayCache{}
	for _, account := range accounts {
		if account == nil || !s.codexCookies.gatewayPoolTakeover(account) {
			continue
		}
		identity, err := s.codexCookies.gatewayPoolIdentity(ctx, account)
		if err != nil {
			return nil, err // an unreadable pool identity is not evidence of recovery
		}
		tag := gatewayPoolLedgerTag(identity)
		peers, loaded := historyPeers[tag]
		if !loaded {
			peers, err = s.gatewayPoolHistoryPeers(ctx, tag)
			if err != nil {
				return nil, err
			}
			historyPeers[tag] = peers
		}
		usageTag := gatewayPoolUsageTag(identity)
		state := readGatewayPoolUsageForIdentity(account, identity)
		stored := readGatewayPoolUsage(account, usageTag)
		// 其它实例可能已更新克隆快照；每次展示读取最新数据库投影，同配置只查一次。
		usagePeers, loaded := usageSnapshots[usageTag]
		if !loaded {
			var legacy bool
			usagePeers, legacy, err = gatewayPoolUsagePeers(ctx, s.accountRepo, identity)
			if err != nil {
				return nil, err
			}
			usageSnapshots[usageTag], usageLegacy[usageTag] = usagePeers, legacy
		}
		legacy := usageLegacy[usageTag]
		if !legacy && stored.UpdatedAt.IsZero() {
			state = stored
		}
		if cached, ok := s.codexCookies.poolUsageCache.Load(usageTag); ok && gatewayPoolUsageCacheAllowed(ctx) {
			if other, valid := cached.(*gatewayPoolUsageLedger); valid && other != nil {
				if legacy || other.UpdatedAt.After(state.UpdatedAt) {
					state = *other
				}
				// 已提交的配置快照优先于旧版本后来写出的共享快照。
				if legacy {
					legacy, usagePeers = false, nil
				}
			}
		}
		for i := range usagePeers {
			other := gatewayPoolUsagePeerView(&usagePeers[i], identity, legacy)
			if other.UpdatedAt.After(state.UpdatedAt) {
				state = other
			}
		}
		// 旧共享域的临时迁移投影仍会变化，不能成为权威配置缓存。
		if !legacy && !state.UpdatedAt.IsZero() && gatewayPoolUsageCacheAllowed(ctx) {
			s.cacheGatewayPoolUsage(usageTag, &state)
		}
		runtime := &GatewayPoolRuntimeView{ObservedAt: time.Now().UTC(), Tickets: []GatewayPoolLiveTicket{},
			Rounds: make([]GatewayPoolUsageRound, len(state.Rounds)), Archived: state.Archived, Incomplete: state.Incomplete}
		runtime.History, runtime.Contacts = s.gatewayPoolDisplaySnapshot(account, identity, peers, displayCache)
		runtime.CooldownEstimate = s.codexCookies.gatewayPoolCooldownEstimate(identity, account, runtime.History, runtime.ObservedAt)
		restTag := gatewayPoolRestTag(identity)
		restRows, loaded := restPeers[restTag]
		if !loaded {
			restRows, err = s.gatewayPoolStatePeers(ctx, restTag, "rest")
			if err != nil {
				return nil, err // an unavailable rest snapshot is not a recovered account
			}
			restPeers[restTag] = restRows
		}
		runtime.Rest = s.gatewayPoolRestDisplay(account, identity, restRows)
		runtime.LedgerTag, runtime.ConcurrencyLimit = tag, account.Concurrency
		runtime.GatewayWindowSeconds = int(account.gatewayPoolGatewayWindow().Seconds())
		if count, known := concurrency[account.ID]; known {
			runtime.CurrentConcurrency = &count
		}
		live := s.codexCookies.gatewayPoolUsageLive(identity)
		session := s.codexCookies.gatewayPoolUsageSession()
		blockedAt := gatewayPoolUsageBlockedAt(account)
		var idleAt time.Time
		inventory := s.codexCookies.gatewayPoolInventory(identity)
		inventory.mu.Lock()
		if inventory.active == 0 && inventory.requests == 0 {
			idleAt = state.idleCutoff(runtime.ObservedAt)
		}
		inventory.mu.Unlock()
		for i := range state.Rounds {
			round := state.Rounds[i] // read-only projection; never mutate cached ticket maps
			if round.Model == gatewayPoolUsageSharedModel && round.EndedAt.IsZero() {
				if !idleAt.IsZero() && !idleAt.Before(round.StartedAt) {
					round.EndedAt, round.EndReason = idleAt, "idle_timeout"
				}
				if !blockedAt.IsZero() && !blockedAt.Before(round.StartedAt) &&
					(round.EndedAt.IsZero() || blockedAt.Before(round.EndedAt)) {
					round.EndedAt, round.EndReason = blockedAt, "temporarily_unschedulable"
				}
			}
			runtime.Rounds[i] = round.fullUsageView(live, session, runtime.ObservedAt)
		}
		if pair, live := s.codexCookies.cachedPoolPair(identity); live == openAIGatewayPoolPairLive {
			ticket := GatewayPoolLiveTicket{Gateway: pair.gateway, Region: pair.region, ExpiresAt: pair.routeExpiresAt, VerifiedModels: []string{}}
			if mark, ok := s.codexCookies.gatewayPoolVerifiedMarkOf(identity); ok && mark.version == pair.version && mark.models != nil {
				ticket.VerifiedAt = mark.at
				for model := range *mark.models {
					ticket.VerifiedModels = append(ticket.VerifiedModels, model)
				}
				sort.Strings(ticket.VerifiedModels)
			}
			runtime.Tickets = append(runtime.Tickets, ticket)
		}
		closedBefore := state.ClosedBefore[gatewayPoolUsageSharedModel]
		for _, boundary := range []time.Time{blockedAt, idleAt} {
			if boundary.After(closedBefore) {
				closedBefore = boundary
			}
		}
		// Scope filtering must precede selection and retention: otherwise a
		// late old run can evict the new cycle's last completed progress.
		progressAccount := account.ID
		sharedProgress := false
		if value, ok := s.codexCookies.poolPrepareProgress.Load(identity); ok {
			if run, ok := value.(*gatewayPoolProgressRun); ok {
				progressAccount = run.account
				sharedProgress = true
			}
		}
		progress := s.codexCookies.poolProgress.snapshot([]int64{progressAccount}, runtime.ObservedAt,
			map[int64]gatewayPoolProgressScope{progressAccount: {tag: usageTag, closedBefore: closedBefore, identity: identity}})[progressAccount]
		if sharedProgress && progress.RunID != "" {
			progress.ActiveRequests = s.codexCookies.gatewayPoolPreparationWaiters(identity)
		}
		if progress.Phase == "" {
			progress.Phase = "idle"
		}
		if progress.Phase == "ready" && !s.codexCookies.gatewayPoolVerifiedFull(identity) {
			// A completed run is historical evidence, not proof that its ticket
			// is still live. Only active preparation may claim to be fetching.
			progress.Phase = "pending"
		}
		progress.Runtime = runtime
		out[account.ID] = progress
	}
	return out, nil
}

func (s *adminServiceImpl) GatewayPoolRuntimeProgress(ctx context.Context, ids []int64) (map[int64]GatewayPoolProgress, error) {
	if reader, ok := s.runtimeBlocker.(interface {
		GatewayPoolRuntimeProgress(context.Context, []int64) (map[int64]GatewayPoolProgress, error)
	}); ok {
		return reader.GatewayPoolRuntimeProgress(ctx, ids)
	}
	return s.GatewayPoolProgress(ids), nil
}
