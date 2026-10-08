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
    // 消息体本身不能是 null，否则后面读 fromAgentId 会 NPE
    if (message == null) {
        throw FlowAgentException.invalidParam("消息体不能为空");
    }

    // ===== 第 1 步：会话准入 =====
    // 会话必须存在、且状态是 OPEN。已关闭/已过期的会话拒收，
    // 内部会调 requireSession 先查存在，再查状态。
    A2aSession session = requireOpenSession(sessionId);

    // ===== 第 2 步：校验发送方 =====
    String from = message.getFromAgentId();
    // fromAgentId 是必填，没填就是调用方请求体写错了
    if (from == null || from.isBlank()) {
        throw FlowAgentException.invalidParam("发送方 fromAgentId 不能为空");
    }
    // 发送方必须在会话成员表里——防止有人冒充一个不在群里的 Agent 发言
    if (!session.hasMember(from)) {
        throw FlowAgentException.of("NOT_A_MEMBER",
                "发送方不在会话成员里: " + from, 403);
    }

    // ===== 第 3 步：校验接收方 =====
    String to = message.getToAgentId();
    // toAgentId 为 null 表示广播，不需要校验；只有指定了具体接收方才查
    if (to != null) {
        // 自己发给自己 = 自环。这不是正常业务会发生的，
        // 防的是配置事故（base-url 配错、成员表传播丢了）
        if (to.equals(from)) {
            throw FlowAgentException.of("SESSION_LOOP",
                    "接收方就是发送方自身，会形成自环: " + from, 409);
        }
        // 接收方也必须在成员表里，否则消息发不出去
        if (!session.hasMember(to)) {
            throw FlowAgentException.of("NOT_A_MEMBER",
                    "接收方不在会话成员里: " + to, 403);
        }
    }

    // ===== 第 4 步：消息预算闸 =====
    // currentSeq 是这个会话已经分配到的最大序号（= 已发消息数）
    // 超过 messageBudget（默认 200）就拒收，防止无限烧 LLM
    long delivered = store.currentSeq(sessionId);
    if (delivered >= messageBudget) {
        throw FlowAgentException.of("MESSAGE_BUDGET_EXCEEDED",
                "会话 " + sessionId + " 已达消息预算 " + messageBudget + " 条", 429);
    }

    // ===== 第 5 步：补服务端默认字段 =====
    // 调用方可能没填 messageId/kind/时间戳/重要性，这里服务端补上。
    // 调用方给的值一律尊重，只补空的。
    fillDefaults(message, sessionId);

    // ===== 第 6 步：回复去重闸 =====
    // guardable = 这条消息是不是"答复类"（REPLY 或 PROPOSE），且带了 replyTo
    // 只有答复类才需要去重——请求类（REQUEST/SOLICIT）重试是正常的，不去重
    boolean guardable = message.getReplyTo() != null && !message.getReplyTo().isBlank()
            && REPLY_DEDUPE_KINDS.contains(message.getKind());
    // claimed 标记"这个去重槽是不是我抢到的"，后面 append 失败要据此还槽
    boolean claimed = false;

    if (guardable) {
        try {
            // 抢槽：返回 null = 我抢到了，可以继续落盘；
            // 返回非 null = 别人先到了，那个返回值就是先到的那条消息
            A2aMessage winner = store.claimReply(message);
            if (winner != null) {
                // 重复消息：不落盘、不投递，直接把先到的那条返回给调用方
                // 计数 +1，让运维知道这道闸真的挡下过东西
                long n = replyDuplicatesSkipped.incrementAndGet();
                log.warn("[A2A] 重复回复已拦下，返回先到的那条 | session={} | from={} | replyTo={} | 累计 {} 条",
                        sessionId, message.getFromAgentId(), message.getReplyTo(), n);
                return winner;
            }
            // 我抢到了槽位
            claimed = true;
        } catch (Exception e) {
            // Redis 挂了——fail-open：不去重了，照常落盘。
            // 但要计数留痕，否则运维不知道这道闸今天其实是坏的。
            // 日志限流：第 1 次和每 100 次打一条，避免刷屏
            long n = replyGuardFailures.incrementAndGet();
            if (n == 1 || n % 100 == 0) {
                log.warn("[A2A] 回复去重闸读不到，本次放行不做去重 | 累计 {} 次 | session={} | replyTo={} | error={}",
                        n, sessionId, message.getReplyTo(), e.toString());
            }
        }
    }

    // ===== 第 7 步：落盘 =====
    long seq;
    try {
        // append 会分配一个会话内单调递增的 seq，并把消息写进 Redis 消息日志
        seq = store.append(message);
    } catch (RuntimeException e) {
        // 落盘失败了。如果刚才抢到了去重槽，必须把槽还回去——
        // 不还的话，调用方重试时永远被"重复"挡住，直到 TTL 到期
        if (claimed) {
            releaseQuietly(message);
        }
        // 原始异常继续往上抛，不吞
        throw e;
    }

    // 如果抢到了槽，落盘时才分配到 seq——
    // 但抢槽时写进槽里的 JSON 还没有 seq，这里补写进去，
    // 否则后来者拿到的重复消息 seq=0，会被当成增量游标从头读整个会话
    if (claimed) {
        refreshQuietly(message);
    }

    // ===== 第 8 步：刷新会话元数据 =====
    // 消息数 +1，更新时间戳，写回会话元数据
    session.setMessageCount((int) seq);
    session.setUpdatedAt(Instant.now());
    store.save(session);

    // ===== 第 9 步：投递 =====
    // 落盘完成了，才开始通知接收方。
    // deliver 内部会判断每个接收方是本实例还是远端，走不同路径。
    deliver(session, message);

    // 返回带了 seq/messageId 的完整消息给调用方
    return message;
}

```

## 消息写完怎么让对方知道

接收方可能在本实例，也可能在远端机器。本实例要绕一圈网络叫醒自己吗？

本实例直接调 `peerDispatcher.dispatch()`（丢线程池就返回）；远端发 HTTP `/wake`，body 里只放 sessionId/messageId/seq——不发消息本体，因为本体已经在 Redis 里了。

```java
/**
 * 投递入口：消息已经落盘了，现在逐个通知接收方
 * 由 post() 在最后一步调用
 */
private void deliver(A2aSession session, A2aMessage message) {
    // recipients() 算出这条消息实际该发给哪些人（广播展开 or 指定单个）
    for (Member member : recipients(session, message)) {
        // 往收件箱放指针（不分本地远端，对每个成员都做）
        // 只有 INBOX_KINDS（INFORM/PROPOSE/VOTE）才进收件箱
        if (INBOX_KINDS.contains(message.getKind())
                && !LOCAL_CONVENER_AGENT_ID.equals(member.getAgentId())) {
            // pushInbox 往 Redis 队列里写一条 "sessionId:seq" 指针，
            // 不存消息本体，本体在消息日志里，指针只是个坐标。
            store.pushInbox(member.getAgentId(), session.getSessionId(), message.getSeq());
        }
        // 通知执行（本地和远端分叉）
        // isSelf 判断这个成员的 endpoint 是不是本实例地址
        if (isSelf(member.getEndpoint())) {
            //
            peerDispatcher.dispatch(session.getSessionId(), message.getMessageId());
        } else {
            wake(session, member, message);
        }
    }
}
```

recipients()

```java
private List<Member> recipients(A2aSession session, A2aMessage message) {
    if (message.getToAgentId() == null) {
        return session.getMembers().stream()
                .filter(m -> !Objects.equals(m.getAgentId(), message.getFromAgentId()))
                .toList();
    }
    Member target = session.member(message.getToAgentId());
    return target == null ? List.of() : List.of(target);
}
```

wake()

```java
private void wake(A2aSession session, Member member, A2aMessage message) {
    String url = member.getEndpoint() + "/sessions/" + session.getSessionId() + "/wake";

    Map<String, Object> body = new LinkedHashMap<>();
    body.put("sessionId", session.getSessionId());
    body.put("messageId", message.getMessageId());
    body.put("seq", message.getSeq());
    body.put("fromAgentId", message.getFromAgentId());
    body.put("kind", message.getKind().name());

    String payload = JsonUtil.toJson(body);

    try {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .header("X-A2A-Session", session.getSessionId())
                .header("X-A2A-Message", message.getMessageId())
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(deliverTimeoutSeconds));

        signature.headersFor(session.getSessionId(), payload).forEach(builder::header);

        HttpResponse<String> response = httpClient.send(builder.build(),
                HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() / 100 != 2) {
            throw FlowAgentException.of("DELIVER_FAILED",
                    "唤醒 " + member.getAgentId() + " 返回 HTTP " + response.statusCode()
                            + "，消息已落盘但未送达", 502);
        }
    } catch (IOException e) {
        throw FlowAgentException.of("DELIVER_FAILED",
                "唤醒 " + member.getAgentId() + " 失败（" + url + "）: " + e.getMessage(), 502);
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw FlowAgentException.of("DELIVER_FAILED",
                "唤醒 " + member.getAgentId() + " 被中断", 502);
    }
}
```

isSelf()

```java
private boolean isSelf(String endpoint) {
    return localBaseUrl != null && !localBaseUrl.isBlank() && endpoint.startsWith(localBaseUrl);
}
```

## 读消息分为几种

读消息分为 messages, inbox

**message() 服务人（和等回复的调用方）**

它的实际调用方：

`DelegationManager.awaitReply()`：委派方在循环里调它，找 `replyTo == 自己那条 messageId` 的 REPLY

`A2aController.readMessages`：人打开网页看 "这个会话从头到尾聊了什么"

这两个场景的共同点是：看多少次都一样，不能看一次就少一条。如果 messages 是取走即消费的，委派方轮询两次，第二次就空了 —— 它怎么知道 "还没收到回复" 还是 "已经收到但被我看没了"？

所以它是幂等的：`seq > afterSeq` 的消息，每次读都一样。

**inbox() 服务模型（ReAct 每轮推理）**

`ReActAgent` 在每轮 Thought 之前，调一次 `GET /a2a/agents/{agentId}/inbox`，把里面的消息当成 "别人新告诉我的事" 塞进提示词。

这个场景的需求就完全反过来了：说过一次就够了

想象一下如果用 `messages()` 给模型喂上下文：

第 1 轮：模型看到 5 条新消息，处理了

第 2 轮：模型又看到这 5 条（因为 messages 是全量的），它以为是新的，又处理一遍

第 3 轮：还是这 5 条……

而且 ReActAgent 的历史窗口有限（默认只留最近 4 步），这些重复消息会把窗口占满，真正重要的早期约束（比如 "别动生产库"）被挤掉

inbox() 取走即消费，正好解决这个问题：每条新消息模型只看一次，看完就从队列里消失，下次看到的都是 "真・新到的"。

另外，也不是所有消息都进收件箱。只有 `INBOX_KINDS = {INFORM, PROPOSE, VOTE}` 这三种：

`REQUEST`/`SOLICIT`：会直接唤醒对方跑 Agent（走 PeerDispatcher），不需要再进收件箱 —— 都已经叫醒你干活了，不用再提醒

`REPLY`：是发起方同步轮询取走的（`awaitReply`），也不需要进收件箱

`INFORM`/`PROPOSE`/`VOTE`：没人在等回复，但下次推理时该让模型知道 —— 所以进收件箱

**为什么不能合并成一个接口**

因为两个需求是矛盾的

| | messages() | inbox() |
|---|---|---|
| 读完之后 | 数据不变 | 数据被删 |
| 多次调用 | 返回相同结果 | 返回不同结果（第二次可能空） |
| 限量 | 不限（全量） | 最多 20 条 |
| 服务谁 | 人复盘 / 等回复的调用方 | 模型每轮推理 |
| 类比 | 聊天记录 | 未读红点 |

messages 是 "真相"，inbox 是 "通知"。真相要可重复读，通知要消费一次就完。

```java
public List<A2aMessage> messages(String sessionId, long afterSeq) {
    requireSession(sessionId);
    return store.readAfter(sessionId, afterSeq);
}
```

```java
public List<A2aMessage> inbox(String agentId, int limit) {
    if (agentId == null || agentId.isBlank()) {
        throw FlowAgentException.invalidParam("agentId 不能为空");
    }
    return store.drainInbox(agentId, Math.min(Math.max(limit, 1), MAX_INBOX_BATCH));
}
```

```java
public Optional<A2aSession> get(String sessionId) {
    return store.find(sessionId);
}

public List<A2aSession> sessionsByRun(String runId, int limit) {
    return store.findByRun(runId, limit);
}

public List<A2aSession> recentSessions(int limit) {
    return store.findRecent(limit);
}

public A2aSession close(String sessionId) {
    A2aSession session = requireSession(sessionId);
    if (session.getStatus() == A2aSession.SessionStatus.OPEN) {
        session.setStatus(A2aSession.SessionStatus.CLOSED);
        session.setUpdatedAt(Instant.now());
        store.save(session);
        log.info("[A2A] session closed | id={} | messages={}",
                sessionId, session.getMessageCount());
    }
    return session;
}
```

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

某个成员不在注册表

会话不存在

往已关闭会话 post

发给自己

发送方/接收方不是成员

消息超200条

**fail-open**

去重闸`claimReply`读不到（Redis抖动）

releaseReply失败（槽没还掉）

refreshReply失败（槽里 seq 没补全）

收件箱指针解析不出消息（会话过期了）

