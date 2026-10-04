package securityaudit

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPromptPayloadEnvelopeRoundTripAndLegacyCompatibility(t *testing.T) {
	encoded, err := encodePromptPayload("送审文本", "完整转录")
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(encoded, payloadEnvelopePrefix))

	decoded, err := decodePromptPayload(encoded)
	require.NoError(t, err)
	require.Equal(t, "送审文本", decoded.ScanText)
	require.Equal(t, "完整转录", decoded.FullPrompt)

	// 历史版本写出的纯文本载荷：只当作送审文本，没有完整转录。
	legacy, err := decodePromptPayload("历史纯文本载荷")
	require.NoError(t, err)
	require.Equal(t, "历史纯文本载荷", legacy.ScanText)
	require.Empty(t, legacy.FullPrompt)

	// 信封内文本本身含前缀/分隔符也要原样还原。
	tricky, err := encodePromptPayload("a"+payloadEnvelopePrefix+"b", "c")
	require.NoError(t, err)
	restored, err := decodePromptPayload(tricky)
	require.NoError(t, err)
	require.Equal(t, "a"+payloadEnvelopePrefix+"b", restored.ScanText)

	_, err = encodePromptPayload("", "完整转录")
	require.Error(t, err)
}

func TestPromptPayloadEnvelopeCorruptionIsAnErrorNotASilentFallback(t *testing.T) {
	// 有信封但 JSON 损坏：必须报错交给 worker 的失败路径，不能退回“把信封当送审文本”。
	_, err := decodePromptPayload(payloadEnvelopePrefix + "{not-json")
	require.Error(t, err)

	_, err = decodePromptPayload(payloadEnvelopePrefix + `{"full_prompt":"只有存证"}`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "scan text")
}
