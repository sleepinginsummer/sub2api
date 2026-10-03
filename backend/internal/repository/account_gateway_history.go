package repository

import (
	"context"
	"encoding/json"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

// mergeLockedGatewayHistoryExtra 只在已有事务中调用，避免旧入口快照覆盖其它请求的落点。
func mergeLockedGatewayHistoryExtra(ctx context.Context, client *dbent.Client, id int64, updates map[string]any) (map[string]any, error) {
	rows, err := client.QueryContext(ctx, "SELECT COALESCE(extra, '{}'::jsonb) FROM accounts WHERE id = $1 AND deleted_at IS NULL FOR UPDATE", id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, service.ErrAccountNotFound
	}
	var encoded []byte
	if err := rows.Scan(&encoded); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	var existing map[string]any
	if err := json.Unmarshal(encoded, &existing); err != nil {
		return nil, err
	}
	return service.MergeOpenAIGatewayHistoryExtra(existing, updates)
}
