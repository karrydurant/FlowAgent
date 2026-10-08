# SessionManager

## 定位

SessionManager 是 A2A 会话协议的 "应用服务层"—— 它把 "跨进程 Agent 怎么开会、怎么发消息、怎么不重复发、怎么通知到对方" 这一整套协议，编排成一组 Java 方法。

## 负责 4 件事

| 类别 | 方法 | 一句话 |
|---|---|---|
| 开会话 | `open` / `openConvened` | 把人凑齐，全过了才开门 |
| 发消息 | `post` | 落盘 + 去重 + 唤醒 |
| 读消息 | `messages` / `inbox` / `get` / `sessionsByRun` | 两种读法，按用途分 |
| 收尾 | `close` | 关门，幂等 |

## 它不管什么

不知道 Redis 数据结构（`getBucket/getList`）—— 全推给 `SessionStore`

不知道 Agent 怎么跑 LLM—— 推给 `PeerDispatcher`

不解析 HTTP 请求体 —— 推给 `A2aController`

不知道消息 payload 里 `task` 是什么 —— 它不关心，那是 `ReActAgent` 的事

自己不存状态 —— 状态全在 Redis

## 准入原则

如果某个 Agent 已经下线了，但会话还是建出来了，会发生什么？—— 有的成员收到消息、有的没收到，调用方拿到一个半截结论。

开会话时逐个把成员验完，任何一个不通过就抛异常，会话根本不创建

## 怎么避免一条回复被写两遍

网络唤醒可能重试两次、PeerDispatcher 可能超时重投，同一条 REPLY 如果写两遍，就多花一次 LLM 的钱。

对 REPLY/PROPOSE 这两种 "答复类" 消息，落盘前先抢一个 Redis 槽，抢到才写，抢不到就把先到那条原样返回。

```java
public A2aMessage post(String sessionId, A2aMessage message) {
    //1.会话必须 open
    A2aSession session=requireOpenSession(sessionId);
    //2.发送方必须是成员
    String from=message.getFromAgentId();
    if (from==null || from.isBlank()) throw invalidParam(...);  
    //3.接收方校验
    //4.消息预算
    //5.补默认字段
    //6.回复去重闸
    boolean guardable=message.getReplyTo() != null && !message.getReplyTo().isBlank()
            && REPLY
    //7.落盘
}
```

## 消息写完怎么让对方知道

接收方可能在本实例，也可能在远端机器。本实例要绕一圈网络叫醒自己吗？

本实例直接调 `peerDispatcher.dispatch()`（丢线程池就返回）；远端发 HTTP `/wake`，body 里只放 sessionId/messageId/seq——不发消息本体，因为本体已经在 Redis 里了。

## 读消息分为几种

人复盘要读完整记录，模型喂上下文只要未读 —— 这俩能合并成一个接口吗？

不能。`messages()` 幂等、全量；`inbox()` 取走即消费、限量 20 条。

## 故障语义

判断一件事该 fail-open 还是 fail-closed：它挂了，是让系统做错事，还是只是让系统多做一次重复的事

| 故障场景 | 行为 | 类型 | 设计理由 |
|---|---|---|---|
| 某成员不在注册表 | open 抛 404，会话不创建 | fail-closed | 准入前置，不留半截会话 |
| acceptsDelegation=false | open 抛 409 | fail-closed | 对方明确不接活 |
| a2aEndpoint 为空 | open 抛 422 | fail-closed | 没地址根本投递不了 |
| 成员数超 8 | open 抛 422 | fail-closed | 成员数=深度上限 |
| 会话不存在 | post 抛 404 | fail-closed | 防笔误 |
| 往已关闭会话 post | post 抛 409 | fail-closed | 状态机硬约束 |
| to==from 自环 | post 抛 409 | fail-closed | 防配置事故导致死循环 |
| 发送/接收方非成员 | post 抛 403 | fail-closed | 防越权 |
| 消息超 200 条 | post 抛 429 | fail-closed | 预算硬上限 |
| 去重闸 Redis 抖动 | 放行落盘，计数+1 | fail-open | 去重是加固不是正确性 |
| releaseReply 失败 | 只 WARN | fail-open | 槽 TTL 后自动清 |
| refreshReply 失败 | 只 WARN | fail-open | 不影响消息本体 |
| 远端唤醒非 2xx | 抛 502 "消息已落盘但未送达" | 分阶段报错 | 记录与通知分开报 |
| 唤醒时被中断 | 还原中断状态再抛 502 | 协作式中断 | 不能吞掉上层取消信号 |
| close 已关闭会话 | 幂等返回 | 幂等设计 | 调用方不必先查状态 |

**fail-closed**

某个成员不在注册表，会话不存在，往已关闭会话 post，发给自己，发送方/接收方不是成员，消息超200条

**fail-open**

去重闸`claimReply`读不到（Redis抖动）

releaseReply失败（槽没还掉）

refreshReply失败（槽里 seq 没补全）

收件箱指针解析不出消息（会话过期了）

