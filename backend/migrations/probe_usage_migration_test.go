package migrations

import (
	"strings"
	"testing"
)

func TestProbeUsageRequestTypeMigrationIsHotTableSafe(t *testing.T) {
	content, err := FS.ReadFile("244_allow_probe_usage_request_type.sql")
	if err != nil {
		t.Fatalf("读取迁移失败: %v", err)
	}
	sql := string(content)
	for _, required := range []string{
		"SET LOCAL lock_timeout = '5s'",
		"CHECK (request_type >= 0 AND request_type <= 6) NOT VALID",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("迁移缺少热表保护: %s", required)
		}
	}
}
