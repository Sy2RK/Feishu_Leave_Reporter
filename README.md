# 飞书请假状态自动同步系统

这个项目会监听飞书原生审批里的请假通过/撤销事件，并通过官方 `calendar/v4/timeoff_events` API 自动为员工写入或删除请假日程，从而让飞书客户端自动展示请假状态。

## 功能概览

- 使用飞书长连接事件订阅，不需要公网回调地址
- 只处理飞书原生审批里的请假控件组事件
- 仅使用官方请假日程能力，不额外写入个人系统状态
- 启动时自动回补最近一段时间内的已通过审批，恢复当前和未来请假状态
- 常驻运行时每天 `08:00` 和 `18:00` 会再做一次全量对账巡检
- 常驻运行时每周一 `09:00` 会向指定飞书群自定义机器人 webhook 推送一张“本周请假预报”卡片
- 使用 SQLite 持久化事件去重、审批片段和已创建的请假日程映射
- 提供 `launchd` 模板、安装脚本、卸载脚本和前台运行脚本

## 前置条件

你需要先在飞书开放平台完成下面这些配置：

- 创建企业自建应用
- 为应用开启机器人能力
- 将事件接收方式配置为长连接
- 在应用里订阅 `leave_approval`、`leave_approvalV2`、`leave_approval_revert`
- 发布应用使配置生效
- 为应用申请并开通这些权限：
  - 审批读取/订阅相关权限
  - 审批定义订阅权限
  - 创建/删除请假日程权限
  - 获取用户 `user_id` 权限 `contact:user.employee_id:readonly`
- 如果要启用周报推送，需要在目标群里额外创建一个**自定义机器人**，并保存其 webhook 地址

周报通过群自定义机器人 webhook 发送，不依赖应用额外 OpenAPI 权限。相关官方说明见：

- 飞书卡片自定义机器人快速开始：https://open.feishu.cn/document/uAjLw4CM/ukzMukzMukzM/feishu-cards/quick-start/send-message-cards-with-custom-bot
- 飞书卡片内容结构：https://open.feishu.cn/document/common-capabilities/message-card/message-cards-content/card-header

应用还必须对目标审批定义调用一次“订阅审批事件”接口。服务启动时会自动尝试执行这一步。

## 环境变量

公开配置接口只使用这些环境变量：

- `FEISHU_APP_ID`
- `FEISHU_APP_SECRET`
- `FEISHU_APPROVAL_CODES`
- `FEISHU_WEEKLY_REPORT_WEBHOOK_URL`
- `FEISHU_TIMEZONE`
- `LOOKBACK_DAYS`
- `DB_PATH`
- `LOG_LEVEL`
- `LAUNCHD_LABEL`

其中只有前 3 项是必填项；`FEISHU_WEEKLY_REPORT_WEBHOOK_URL` 不填时会关闭周报推送，其它变量都有默认值。

## 快速开始

1. 安装依赖：

```bash
uv sync
```

2. 复制环境变量模板并填写：

```bash
cp .env.example .env
```

3. 前台运行服务：

```bash
scripts/run_service.sh
```

## `.env` 示例

参考 [`.env.example`](./.env.example)。

## launchd 部署

安装并启动：

```bash
scripts/install_launchd.sh
```

卸载并移除：

```bash
scripts/uninstall_launchd.sh
```

安装脚本会：

- 运行 `uv sync`
- 生成 `launchd` plist
- 安装到 `~/Library/LaunchAgents`
- 通过 `launchctl bootstrap` 启动服务

## 运行时文件

默认会创建这些运行时目录：

- 数据库：`var/state/leave-sync.db`
- Python 日志文件：`var/log/feishu-leave-sync.log`
- `launchd` stdout：`var/log/feishu-leave-sync.stdout.log`
- `launchd` stderr：`var/log/feishu-leave-sync.stderr.log`

## 设计说明

### 事件处理策略

- `leave_approval`：兼容兜底创建路径；若后续收到 `leave_approvalV2`，则以 V2 明细覆盖
- `leave_approvalV2`：优先创建路径，支持按 `leave_range` 拆分多时段
- `leave_approval_revert`：真实删除路径

### 巡检策略

- 启动后立刻执行一次全量回补对账
- 服务常驻期间依然通过长连接实时处理审批通过和撤销事件
- 除实时事件外，每天 `08:00` 与 `18:00` 会固定再执行一次全量对账，用来兜底修复离线期或偶发漏推送
- 如果配置了 `FEISHU_WEEKLY_REPORT_WEBHOOK_URL`，每周一 `09:00` 会向群聊推送一张飞书卡片，汇总“本周仍未结束的已审批请假”

### 请假片段策略

实时事件优先解析 `leave_range`，会把多个离散请假时段拆成多条请假日程。

启动回补使用审批实例详情里的 `leaveGroupV2` 起止时间做恢复。这个路径能可靠恢复开始和结束时间，但如果服务离线期间审批被拆成多个离散工作时段，回补会按连续区间恢复，实时事件路径仍会以 `leave_range` 为准。

### 幂等策略

- 所有事件按 `uuid` 去重
- 请假片段按 `instance_code + start_time + end_time` 唯一
- 已创建的 `timeoff_event_id` 单独持久化
- 周报按“周一日期”持久化发送记录，避免同一周重复推送

## 测试

运行测试：

```bash
uv run pytest
```

## 常见排查

- 收不到事件：确认应用事件订阅已发布，并且审批定义已经被服务成功订阅
- 启动失败：确认 `.env` 中的 `FEISHU_APP_ID`、`FEISHU_APP_SECRET`、`FEISHU_APPROVAL_CODES` 有值
- 创建请假日程失败：确认应用开启了机器人能力，并且拿到了创建/删除请假日程权限
- 如果日志报 `open_id cross app`：说明该用户的事件 `open_id` 不能直接用于创建请假日程，服务会自动优先改用 `user_id`
- 如果日志报缺少 `contact:user.employee_id:readonly`：需要在开放平台为应用开通该权限、发布生效后再重启服务
- 周报没有发出：确认 `.env` 中已配置 `FEISHU_WEEKLY_REPORT_WEBHOOK_URL`，并且目标群自定义机器人没有额外开启签名校验或 IP 白名单限制
