//go:build unit

package service

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

// 两个前置检查都读取最后一个名额；原子创建接口才有权决定哪一个请求占用它。
type atomicCreateLimitRepoStub struct {
	*createLimitAPIKeyRepoStub
	arrived atomic.Int64
	barrier chan struct{}
	mu      sync.Mutex
	count   int
}

func (r *atomicCreateLimitRepoStub) CountByUserID(context.Context, int64) (int64, error) {
	if r.arrived.Add(1) == 2 {
		close(r.barrier)
	}
	<-r.barrier
	return 1, nil
}
func (r *atomicCreateLimitRepoStub) Create(context.Context, *APIKey) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.count++
	return nil
}
func (r *atomicCreateLimitRepoStub) CreateWithLimit(_ context.Context, _ *APIKey, limit int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if limit > 0 && r.count >= limit {
		return ErrAPIKeyCountExceeded
	}
	r.count++
	return nil
}
func TestAPIKeyServiceCreateAtomicLimitAfterConcurrentPrechecks(t *testing.T) {
	base, _ := newCreateLimitStubs()
	repo := &atomicCreateLimitRepoStub{createLimitAPIKeyRepoStub: base, barrier: make(chan struct{}), count: 1}
	cfg := &config.Config{}
	cfg.APIKeyCreate.MaxActivePerUser = 2
	svc := &APIKeyService{apiKeyRepo: repo, userRepo: &userRepoStub{user: &User{ID: 7}}, cfg: cfg}
	done := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := svc.Create(context.Background(), 7, CreateAPIKeyRequest{Name: "last-slot"})
			done <- err
		}()
	}
	admitted := 0
	for range 2 {
		err := <-done
		if err == nil {
			admitted++
		} else {
			require.ErrorIs(t, err, ErrAPIKeyCountExceeded)
		}
	}
	require.Equal(t, 1, admitted)
	require.Equal(t, 2, repo.count)
}
