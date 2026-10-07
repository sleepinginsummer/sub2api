//go:build integration

package repository

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type gatewayUsageIntegrationSnapshot struct {
	Tag       string                         `json:"tag"`
	UpdatedAt time.Time                      `json:"updated_at"`
	Rounds    []gatewayUsageIntegrationRound `json:"rounds"`
}

type gatewayUsageIntegrationRound struct {
	Model     string         `json:"model"`
	Attempted int            `json:"attempted"`
	Tickets   map[string]any `json:"tickets"`
}

func gatewayUsageIntegrationNote(ctx context.Context, repo service.AccountRepository, id int64, tag, ticket string) error {
	peers, err := repo.(*accountRepository).FindGatewayPoolStatePeers(ctx, tag, "usage")
	if err != nil {
		return err
	}
	state := gatewayUsageIntegrationSnapshot{Tag: tag}
	for _, peer := range peers {
		raw, err := json.Marshal(peer.Extra["openai_gwpool_usage_rounds"])
		if err != nil {
			return err
		}
		var other gatewayUsageIntegrationSnapshot
		if err := json.Unmarshal(raw, &other); err != nil {
			return err
		}
		if other.UpdatedAt.After(state.UpdatedAt) {
			state = other
		}
	}
	if len(state.Rounds) == 0 {
		state.Rounds = []gatewayUsageIntegrationRound{{Model: "all", Tickets: map[string]any{}}}
	}
	state.Rounds[0].Tickets[ticket] = map[string]any{"full": true}
	state.Rounds[0].Attempted = len(state.Rounds[0].Tickets)
	updated := time.Now().UTC()
	if !updated.After(state.UpdatedAt) {
		updated = state.UpdatedAt.Add(time.Nanosecond)
	}
	state.UpdatedAt = updated
	return repo.UpdateExtra(ctx, id, map[string]any{"openai_gwpool_usage_rounds": state, "openai_gwpool_usage_tag": tag})
}

func TestGatewayUsageTransactionsKeepSameRowAndCloneTickets(t *testing.T) {
	for _, clones := range []bool{false, true} {
		name := "same_row"
		if clones {
			name = "clones"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			tag := "usage-transaction-" + name
			makeAccount := func() *service.Account {
				account := mustCreateAccount(t, integrationEntClient, &service.Account{Name: tag, Platform: service.PlatformOpenAI,
					Extra: map[string]any{"unrelated": "preserved", "openai_gwpool_usage_tag": tag,
						"openai_gwpool_usage_rounds": gatewayUsageIntegrationSnapshot{Tag: tag, UpdatedAt: time.Now().Add(-time.Minute)}}})
				t.Cleanup(func() {
					require.NoError(t, integrationEntClient.Account.DeleteOneID(account.ID).Exec(context.Background()))
				})
				return account
			}
			first := makeAccount()
			second := first
			if clones {
				second = makeAccount()
			}
			start := make(chan struct{})
			results := make(chan error, 2)
			for i, account := range []*service.Account{first, second} {
				ticket := []string{"one", "two"}[i]
				go func(id int64, ticket string) {
					<-start
					repo := newAccountRepositoryWithSQL(integrationEntClient, integrationDB, nil)
					committed, err := repo.WithGatewayPoolUsageTransaction(ctx, tag, id, func(txCtx context.Context, scoped service.AccountRepository) error {
						return gatewayUsageIntegrationNote(txCtx, scoped, id, tag, ticket)
					})
					if err == nil && !committed {
						err = errors.New("owned transaction not committed")
					}
					results <- err
				}(account.ID, ticket)
			}
			close(start)
			require.NoError(t, <-results)
			require.NoError(t, <-results)
			repo := newAccountRepositoryWithSQL(integrationEntClient, integrationDB, nil)
			peers, err := repo.FindGatewayPoolStatePeers(ctx, tag, "usage")
			require.NoError(t, err)
			var newest gatewayUsageIntegrationSnapshot
			for _, peer := range peers {
				raw, err := json.Marshal(peer.Extra["openai_gwpool_usage_rounds"])
				require.NoError(t, err)
				var snapshot gatewayUsageIntegrationSnapshot
				require.NoError(t, json.Unmarshal(raw, &snapshot))
				if snapshot.UpdatedAt.After(newest.UpdatedAt) {
					newest = snapshot
				}
				account, err := repo.GetByID(ctx, peer.ID)
				require.NoError(t, err)
				require.Equal(t, "preserved", account.Extra["unrelated"])
			}
			require.Len(t, newest.Rounds, 1)
			require.Equal(t, 2, newest.Rounds[0].Attempted)
			require.Contains(t, newest.Rounds[0].Tickets, "one")
			require.Contains(t, newest.Rounds[0].Tickets, "two")
		})
	}
}

func TestGatewayUsageCallerTransactionLocksCloneDomainUntilCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tag := "usage-caller-clones"
	accounts := make([]*service.Account, 2)
	for i := range accounts {
		account := mustCreateAccount(t, integrationEntClient, &service.Account{Name: tag, Platform: service.PlatformOpenAI})
		accounts[i] = account
		t.Cleanup(func() {
			// 不同配置写入产生真实调度事件，按夹具账号清理，避免污染后续全表断言。
			_, err := integrationDB.ExecContext(context.Background(), "DELETE FROM scheduler_outbox WHERE account_id = $1", account.ID)
			require.NoError(t, err)
			require.NoError(t, integrationEntClient.Account.DeleteOneID(account.ID).Exec(context.Background()))
		})
	}
	repo := newAccountRepositoryWithSQL(integrationEntClient, integrationDB, nil)
	tx, err := integrationEntClient.Tx(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	txCtx := dbent.NewTxContext(ctx, tx)
	// 事务仓储必须能读到调用方先前尚未提交的修改。
	require.NoError(t, tx.Client().Account.UpdateOneID(accounts[0].ID).SetExtra(map[string]any{"marker": "pending"}).Exec(ctx))
	committed, err := repo.WithGatewayPoolUsageTransaction(txCtx, tag, accounts[0].ID, func(scopedCtx context.Context, scoped service.AccountRepository) error {
		own, err := scoped.GetByID(scopedCtx, accounts[0].ID)
		if err != nil {
			return err
		}
		if own.Extra["marker"] != "pending" {
			return errors.New("read escaped caller transaction")
		}
		return gatewayUsageIntegrationNote(scopedCtx, scoped, own.ID, tag, "one")
	})
	require.NoError(t, err)
	require.False(t, committed)
	entered := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		other := newAccountRepositoryWithSQL(integrationEntClient, integrationDB, nil)
		_, err := other.WithGatewayPoolUsageTransaction(ctx, tag, accounts[1].ID, func(scopedCtx context.Context, scoped service.AccountRepository) error {
			close(entered)
			return gatewayUsageIntegrationNote(scopedCtx, scoped, accounts[1].ID, tag, "two")
		})
		result <- err
	}()
	// 用不同账号行排除行锁的影响，直接证明数据库周期锁跨仓储且持续到调用方提交。
	hash := uint64(advisoryLockHash("gwpool-usage:" + tag))
	require.Eventually(t, func() bool {
		var waiting bool
		err := integrationDB.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND classid::bigint=$1 AND objid::bigint=$2 AND objsubid=1)`, int64(uint32(hash>>32)), int64(uint32(hash))).Scan(&waiting)
		return err == nil && waiting
	}, 2*time.Second, 10*time.Millisecond)
	select {
	case <-entered:
		t.Fatal("clone callback ran before caller transaction committed")
	default:
	}
	// 同配置排队必须遵守超时；不同配置不能被这个周期锁阻塞。
	shortCtx, stop := context.WithTimeout(ctx, 50*time.Millisecond)
	started := time.Now()
	called := false
	committed, waitErr := repo.WithGatewayPoolUsageTransaction(shortCtx, tag, accounts[1].ID, func(context.Context, service.AccountRepository) error {
		called = true
		return nil
	})
	stop()
	require.Error(t, waitErr)
	require.False(t, committed)
	require.False(t, called)
	require.Less(t, time.Since(started), time.Second)
	committed, err = repo.WithGatewayPoolUsageTransaction(ctx, tag+"-other-config", accounts[1].ID, func(scopedCtx context.Context, scoped service.AccountRepository) error {
		return scoped.UpdateExtra(scopedCtx, accounts[1].ID, map[string]any{"other_config": "ready"})
	})
	require.NoError(t, err)
	require.True(t, committed)
	require.NoError(t, tx.Commit())
	require.NoError(t, <-result)
	own, err := repo.GetByID(ctx, accounts[1].ID)
	require.NoError(t, err)
	raw, err := json.Marshal(own.Extra["openai_gwpool_usage_rounds"])
	require.NoError(t, err)
	var state gatewayUsageIntegrationSnapshot
	require.NoError(t, json.Unmarshal(raw, &state))
	require.Equal(t, 2, state.Rounds[0].Attempted)
}

func TestGatewayUsageTransactionRollsBackMutation(t *testing.T) {
	ctx := context.Background()
	account := mustCreateAccount(t, integrationEntClient, &service.Account{Name: "usage-rollback", Platform: service.PlatformOpenAI,
		Extra: map[string]any{"unrelated": "preserved"}})
	t.Cleanup(func() { require.NoError(t, integrationEntClient.Account.DeleteOneID(account.ID).Exec(ctx)) })
	repo := newAccountRepositoryWithSQL(integrationEntClient, integrationDB, nil)
	aborted := errors.New("abort mutation")
	committed, err := repo.WithGatewayPoolUsageTransaction(ctx, "usage-rollback", account.ID, func(txCtx context.Context, scoped service.AccountRepository) error {
		if err := gatewayUsageIntegrationNote(txCtx, scoped, account.ID, "usage-rollback", "discarded"); err != nil {
			return err
		}
		return aborted
	})
	require.ErrorIs(t, err, aborted)
	require.False(t, committed)
	fresh, err := repo.GetByID(ctx, account.ID)
	require.NoError(t, err)
	require.Equal(t, "preserved", fresh.Extra["unrelated"])
	require.NotContains(t, fresh.Extra, "openai_gwpool_usage_rounds")
}
