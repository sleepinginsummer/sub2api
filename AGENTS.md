# AGENTS

本仓库是 Wei-Shaw/sub2api 的 fork，当前开发分支为 `sleepinsum`。

- 续接工作先看 `docs/dev-journal.md` 最新条目；非平凡改动后回写 journal。
- 网关池从 `v0.2.15-klno.3` 起按 KlN-4096/sub2api 对应版本实现，不再保留本地配置隔离、周期事务记账、历史原子合并、请求路由快照和相关缓存补丁；升级时不要重新引入这些旧补丁。
- 网关池运行账本使用迁移 253 的 `gateway_pool_ledgers` 表，旧 `Previous` 归档按迁移 251 清理；接口、迁移与回归测试沿用对应上游版本。
- 独立于网关池的安全审计、API Key 限额、旧迁移兼容与 fork 发布/安装/更新来源继续保留。
- 补丁必须小、可 rebase；协议事实以 openai/codex 源码为准。
- `docs/conventions/` 下此前引用的约定文件与 `docs/tasks/` 当前缺失，不据此虚构额外要求。
