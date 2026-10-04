package securityaudit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// PayloadStore 保存排队任务的瞬态载荷：一次写入、读取后删除。
// 值对存储实现是不透明字符串，格式由 encodePromptPayload/decodePromptPayload 决定。
type PayloadStore interface {
	Set(ctx context.Context, jobID int64, payload string, ttl time.Duration) error
	Get(ctx context.Context, jobID int64) (string, error)
	Delete(ctx context.Context, jobID int64) error
	Ping(ctx context.Context) error
}

// promptPayload 是「入队 → worker」之间传递的审计载荷：送审文本与完整转录分开携带。
// 送审文本可能被收窄（例如只审最新 user 轮），完整转录用于事件存证；两者必须在同一次写入中落盘，
// 否则存证是否完整会取决于「第二次写入是否成功」。
type promptPayload struct {
	ScanText   string `json:"scan_text"`
	FullPrompt string `json:"full_prompt,omitempty"`
}

// payloadEnvelopePrefix 让解码层区分「信封」与历史版本写入的纯文本载荷。
// 历史格式没有信封，解码后只有送审文本（无完整转录时事件存证退化为送审文本，与收窄前一致）。
const payloadEnvelopePrefix = "\x00SUB2API_PROMPT_AUDIT_PAYLOAD_V1\x00"

func encodePromptPayload(scanText, fullPrompt string) (string, error) {
	if scanText == "" {
		return "", errors.New("prompt audit payload requires scan text")
	}
	encoded, err := json.Marshal(promptPayload{ScanText: scanText, FullPrompt: fullPrompt})
	if err != nil {
		return "", fmt.Errorf("prompt audit payload encode failed: %w", err)
	}
	return payloadEnvelopePrefix + string(encoded), nil
}

// decodePromptPayload 处理两种值：带信封的载荷（本版本），以及历史版本写入的纯文本载荷。
// 只有「无信封」的历史载荷会降级为“仅送审文本”；带信封但内容损坏属于新格式自身故障，
// 返回错误交给 worker 的既有失败路径，避免静默改变送审范围与存证语义。
func decodePromptPayload(value string) (promptPayload, error) {
	if !strings.HasPrefix(value, payloadEnvelopePrefix) {
		return promptPayload{ScanText: value}, nil
	}
	var decoded promptPayload
	if err := json.Unmarshal([]byte(strings.TrimPrefix(value, payloadEnvelopePrefix)), &decoded); err != nil {
		return promptPayload{}, fmt.Errorf("prompt audit payload envelope invalid: %w", err)
	}
	if decoded.ScanText == "" {
		return promptPayload{}, errors.New("prompt audit payload envelope missing scan text")
	}
	return decoded, nil
}

type RedisPayloadStore struct {
	client *redis.Client
}

func NewRedisPayloadStore(client *redis.Client) *RedisPayloadStore {
	return &RedisPayloadStore{client: client}
}

func (s *RedisPayloadStore) Set(ctx context.Context, jobID int64, payload string, ttl time.Duration) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("prompt audit payload store unavailable")
	}
	if jobID <= 0 || payload == "" {
		return fmt.Errorf("prompt audit payload input invalid")
	}
	if ttl <= 0 || ttl > DefaultPayloadTTL {
		ttl = DefaultPayloadTTL
	}
	return s.client.Set(ctx, payloadKey(jobID), payload, ttl).Err()
}

func (s *RedisPayloadStore) Get(ctx context.Context, jobID int64) (string, error) {
	if s == nil || s.client == nil {
		return "", fmt.Errorf("prompt audit payload store unavailable")
	}
	return s.client.Get(ctx, payloadKey(jobID)).Result()
}

func (s *RedisPayloadStore) Delete(ctx context.Context, jobID int64) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("prompt audit payload store unavailable")
	}
	return s.client.Del(ctx, payloadKey(jobID)).Err()
}

func (s *RedisPayloadStore) Ping(ctx context.Context) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("prompt audit payload store unavailable")
	}
	return s.client.Ping(ctx).Err()
}

func payloadKey(jobID int64) string {
	return PayloadKeyPrefix + strconv.FormatInt(jobID, 10)
}
