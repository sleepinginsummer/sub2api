package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

// 拒绝凭据的退避只能阻止对应池配置，不能阻止同身份的其它有效池。
func TestGatewayPoolBackoffSeparatesAccountConfigurations(t *testing.T) {
	poolA := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	poolA.refuseStatus = http.StatusServiceUnavailable
	poolA.refuseCode = gwpool.CodeConsumerRejected
	poolB := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-84"), 150)
	store := &openAICodexCookieStore{}
	a, b := poolA.account(1), poolB.account(2)
	require.ErrorIs(t, attachRoute(context.Background(), store, a, gwpoolTestURL, http.Header{}), gwpool.ErrNoSlot)
	headers := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, b, gwpoolTestURL, headers))
	require.Equal(t, "unified-84", openAICodexRouteGateway(headers.Get("Cookie")))
	require.EqualValues(t, 1, poolB.hits.Load())
}

// 两个池可以使用相同票号；还票只删除取票池配置下的缓存。
func TestGatewayPoolReleaseSeparatesAccountConfigurations(t *testing.T) {
	poolA := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	poolB := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-84"), 150)
	store := &openAICodexCookieStore{}
	a, b := poolA.account(1), poolB.account(2)
	release, err := store.AttachRoute(context.Background(), a, gwpoolTestURL, http.Header{})
	require.NoError(t, err)
	require.NotNil(t, release)
	require.NoError(t, attachRoute(context.Background(), store, b, gwpoolTestURL, http.Header{}))
	release()
	require.Equal(t, `{"cookie_version":"tkt-1"}`, poolA.nextRelease(t))
	_, stateA := store.cachedPoolPair(openAIGatewayPoolCacheKey(a, gwpoolTestIdentity))
	require.Equal(t, openAIGatewayPoolPairNone, stateA)
	_, stateB := store.cachedPoolPair(openAIGatewayPoolCacheKey(b, gwpoolTestIdentity))
	require.Equal(t, openAIGatewayPoolPairLive, stateB)
	require.Zero(t, poolB.releaseHits.Load())
}

func TestGatewayPoolCacheSeparatesAccountConfigurations(t *testing.T) {
	first := gwpoolTestPairCookie(t, "unified-142")
	second := gwpoolTestPairCookie(t, "unified-84")
	poolA := newGwpoolFakePool(t, first, 150)
	poolB := newGwpoolFakePool(t, second, 150)
	store := &openAICodexCookieStore{}
	a, b := poolA.account(1), poolB.account(2)
	headers := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, a, gwpoolTestURL, headers))
	other := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, b, gwpoolTestURL, other))
	require.Equal(t, first, headers.Get("Cookie"))
	require.Equal(t, second, other.Get("Cookie"))
	require.EqualValues(t, 1, poolB.hits.Load())
	// 修改同一账号的配置也必须立即切到新池，不能等旧缓存过期。
	poolB.configure(a)
	switched := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, a, gwpoolTestURL, switched))
	require.Equal(t, second, switched.Get("Cookie"))
	require.EqualValues(t, 1, poolB.hits.Load(), "相同身份和配置仍复用缓存")
}

func TestGatewayPoolCacheSeparatesConsumerKeys(t *testing.T) {
	first, second := gwpoolTestPairCookie(t, "unified-142"), gwpoolTestPairCookie(t, "unified-84")
	calls := make(chan string, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		calls <- auth
		pair := first
		if auth == "Bearer second-key" {
			pair = second
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"cookie": pair, "valid_for_s": 150})
	}))
	defer srv.Close()
	store := &openAICodexCookieStore{}
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolSteeringExtraKey] = false
	account.Extra[openAIGatewayPoolBaseURLExtraKey] = srv.URL
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "first-key"
	require.NoError(t, attachRoute(context.Background(), store, account, gwpoolTestURL, http.Header{}))
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "second-key"
	headers := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, account, gwpoolTestURL, headers))
	require.Equal(t, second, headers.Get("Cookie"))
	require.Len(t, calls, 2)
	require.Equal(t, "Bearer first-key", <-calls)
	require.Equal(t, "Bearer second-key", <-calls)
}

func TestGatewayPoolCacheDoesNotBypassInvalidConfiguration(t *testing.T) {
	for _, missing := range []string{openAIGatewayPoolBaseURLExtraKey, OpenAIGatewayPoolConsumerKeyExtraKey} {
		t.Run(missing, func(t *testing.T) {
			pool := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			store := &openAICodexCookieStore{}
			a, b := pool.account(1), pool.account(2)
			require.NoError(t, attachRoute(context.Background(), store, a, gwpoolTestURL, http.Header{}))
			delete(b.Extra, missing)
			headers := http.Header{}
			require.ErrorIs(t, attachRoute(context.Background(), store, b, gwpoolTestURL, headers), gwpool.ErrPool)
			require.Empty(t, headers.Get("Cookie"))
		})
	}
}

func TestGatewayPoolConcurrentDifferentConfigurationsDoNotCoalesce(t *testing.T) {
	entered := make(chan string, 2)
	release := make(chan struct{})
	makePool := func(pair string) *httptest.Server {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			entered <- pair
			<-release
			_ = json.NewEncoder(w).Encode(map[string]any{"cookie": pair, "valid_for_s": 150})
		}))
		t.Cleanup(srv.Close)
		return srv
	}
	first, second := gwpoolTestPairCookie(t, "unified-142"), gwpoolTestPairCookie(t, "unified-84")
	poolA, poolB := makePool(first), makePool(second)
	// 先释放所有被阻塞的请求，再由 Cleanup 关闭测试服务器。
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	store := &openAICodexCookieStore{}
	results := make(chan error, 2)
	for i, url := range []string{poolA.URL, poolB.URL} {
		account := gwpoolTestAccount(int64(i + 1))
		account.Extra[openAIGatewayPoolSteeringExtraKey] = false
		account.Extra[openAIGatewayPoolBaseURLExtraKey] = url
		account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "key"
		go func() { results <- attachRoute(context.Background(), store, account, gwpoolTestURL, http.Header{}) }()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("不同配置被错误合并，只有一个池收到请求")
		}
	}
	unblock()
	for range 2 {
		require.NoError(t, <-results)
	}
}

func TestUpdateAccountTypeValidatesGatewayPoolFinalConfiguration(t *testing.T) {
	for _, configured := range []bool{false, true} {
		t.Run(map[bool]string{false: "invalid", true: "valid"}[configured], func(t *testing.T) {
			const id int64 = 202
			extra := map[string]any{openAIGatewayPoolExtraKey: true}
			if configured {
				extra[openAIGatewayPoolBaseURLExtraKey] = "http://127.0.0.1:8099"
				extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "stored-key"
			}
			repo := &upstreamBillingProbeAccountRepo{accounts: map[int64]*Account{
				id: {ID: id, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"access_token": "offline"}, Extra: extra},
			}}
			svc := &adminServiceImpl{accountRepo: repo}
			updated, err := svc.UpdateAccount(context.Background(), id, &UpdateAccountInput{Type: AccountTypeOAuth})
			if !configured {
				require.Error(t, err)
				require.Equal(t, AccountTypeAPIKey, repo.accounts[id].Type, "校验失败不能写入类型变更")
				return
			}
			require.NoError(t, err)
			require.True(t, updated.UsesGatewayPool())
			require.Equal(t, "stored-key", updated.gatewayPoolConsumerKey())
		})
	}
}
