# A2aController

## 角色定位

它是一栋写字楼的大堂前台。你要办事（开会话、发消息、委派任务、查状态），所有请求都从大门进来，前台先核验身份、看你带没带材料，然后把你领到对应的部门（SessionManager / DelegationManager / NegotiationManager）。前台自己不替你干活。

但它不是一个 "无脑转发" 的前台，它还承担了三个只有它能干的事：

1.接外部 HTTP 请求 -- 把 JSON 字符串翻译成 Java 对象

2.验签 -- 别的实例发过来的请求，证明 "你真是我认识的那个实例"

3.暴露健康指标 -- 把下面各组件的计数器汇总成一个 `/health` 端点

## 暴露了哪些 API -- 分为 4 组

### A：会话底座

| 方法 | 路径 | 干嘛 | 验签 |
|------|------|------|------|
| POST | `/a2a/sessions` | 开会话 | 否 |
| GET  | `/a2a/sessions?runId=&limit=` | 列会话（按 runId 反查 / 列最近） | 否 |
| POST | `/a2a/sessions/{id}/messages` | 投一条消息 | 是 |
| GET  | `/a2a/sessions/{id}/messages?after=N` | 增量读消息 | 否 |
| GET  | `/a2a/sessions/{id}` | 读会话状态 | 否 |
| GET  | `/a2a/agents/{agentId}/inbox` | 取走该 Agent 收件箱（消费型） | 否（特意不验） |
| POST | `/a2a/sessions/{id}/close` | 收尾会话 | 是 |
| POST | `/a2a/sessions/{id}/wake` | 唤醒：别的实例通知本实例"有新消息" | 是 |

### B：委派兼容入口

| 方法 | 路径 | 干嘛 |
|------|------|------|
| POST | `/a2a/delegate` | 一句话委派（内部开个会话、投 REQUEST、等 REPLY） |
| GET  | `/a2a/tasks/{taskId}` | 查委派任务状态 |
| POST | `/a2a/tasks/{taskId}/cancel` | 取消委派 |

### C：协商兼容入口

| 方法 | 路径 | 干嘛 |
|------|------|------|
| POST | `/a2a/negotiate` | 5 种模式：voting / consensus / weighted / supervisor / panel |

### D：发现+健康

| 方法 | 路径 | 干嘛 |
|------|------|------|
| GET | `/a2a/agents?capability=` | 列已注册 Agent |
| GET | `/a2a/dispatch/health` | 暴露四道去重闸的计数器 |


