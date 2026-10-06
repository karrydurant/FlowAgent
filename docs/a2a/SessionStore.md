# SessionStore

## 定位

这是 FlowAgent 多智能体体系中 A2A 会话的 Redis 存储层，基于 Redisson 客户端实现，完整封装了会话元数据、消息日志、序号分配、回复去重、Agent 收件箱五大核心能力。

## 为什么必须用 Redis，不用内存 Map

A2A 会话天然是跨进程的：发起方 Agent 和成员 Agent 可能部署在不同服务实例上，收件箱不共享就根本无法投递消息。同时避免了「内存 + Redis 双份数据」导致的状态漂移、重启后任务丢失、内存泄漏等问题。

## 三键同生共死原则

同一会话对应三组Redis key：

·会话元数据(a2a:session:)

·消息日志(a2a:session:msgs:)

·序号计数器(a2a:session:seq:)

每次写入必须同步续期三者的 TTL，防止出现「消息还在、计数器重置」的情况 —— 一旦计数器从 1 重新分配，两条消息会撞上同一个序号，消费方的增量拉取会静默漏消息。

## TTL 分层设计

会话 TTL 默认 24 小时：作为协作留痕，支持人工隔天复盘

记忆 TTL 默认 30 分钟：作为模型工作集，越短越省资源
两者刻意做了时长区分，适配不同的业务语义。

## 会话元数据与反查索引

负责会话的增删查，以及两个维度的索引维护

| 方法 | 功能 | 关键设计 |
| ---- | ---- | ---- |
| save(A2aSession) | 写入/覆盖会话元数据，同步更新索引并续期 |  |
| find(String) | 读取单个会话 |  |
| remove(String) | 彻底清理会话所有资源 |  |
|  |  |  |

**索引机制**

维护了两个反查索引，且每次 save 都会同步写入并续期：

1.按 runId 分组索引（RSet）：A2A_RUN_SESSIONS 前缀，用于根据一次运行 ID 查到所有关联会话

2.全局最近索引（RScoredSortedSet/ZSet）：A2A_SESSIONS_RECENT 前缀，score 为创建时间戳，支持按时间倒序分页

## 回复去重（幂等防护）

解决分布式场景下重复投递、重复回复的核心问题。

``` java
public A2aMessage claimReply(A2aMessage message) {
    RBucket<String> slot=redissonClient.getBucket(replyKey(message));
    String mine=JSON.toJSONString(message);
    // trySet只有槽不存在的情况才写入，成功返回 true
    if (trySet(slot, mine)) {
        return null;
    }
    String winner=slot.get();
    // 抢到槽位返回 null，代表还没有写入过相同信息
    if (winner==null) {
        // 极端边界处理：`trySet` 返回 false 和 `get` 执行之间存在极短的时间差
        // 刚好槽的 TTL 到期、key 被自动删除了。这时候槽其实又空了，但 `trySet` 已经返回了 false。
        return trySet(slot, mine) ? null : parse(slot.get());
    }
    // 把赢家的 JSON 反序列化成消息对象返回
    return parse(winner);
}

// claimReply 发生在 append 之前，此时消息还没有分配 seq 序号（seq 是在 append 中分配的）
// 槽里存的是 seq=0 的临时版本
// 如果不更新，后来的并发请求拿到的就是错误序号。
// refresh 就是在 A2aMessage 已经带上正确 seq 之后再将它写入
public void refreshReply(A2aMessage message) {
    redissonClient.getBucket(replyKey(message))
            .set(JSON.toJSONString(message), ttl());
}

// 落盘失败时把占用的槽还回去
public void releaseReply(A2aMessage message) {
    redissonClient.getBucket(replyKey(message)).delete();
}

private static A2aMessage parse(String json) {
    return json=null ? null : JSON.parseObject(json, A2aMessage.class);
}

private boolean trySet(RBucket<String> slot, String value) {
    // 不能使用 set + expire：如果写完值、还没设过期时进程崩溃，这个槽会永久占用，永远挡住后续所有同内容回复
    return slot.trySet(value, sessionTtlSeconds, TimeUnit.SECONDS);
}

private static String replyKey(A2aMessage message) {
    return CacheConstants.A2A_SESSION_REPLIED
        + message.getSessionId() + ":"
        + message.getFromAgentId() + ":"
        + message.getReplyTo() + ":"
        + digest(message);
}

private static String digest(A2aMessage message) {
    try {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] hash = md.digest(JSON.toJSONString(message.getPayload())
                .getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder(16);
        for (int i = 0; i < 8; i++) {
            sb.append(Character.forDigit((hash[i] >> 4) & 0xF, 16))
                    .append(Character.forDigit(hash[i] & 0xF, 16));
        }
        return sb.toString();
    } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException("JRE 缺少 SHA-256，回复去重闸不可用", e);
    }
}
```

**核心思路**：原子认领槽

为每一条可能重复的回复构造一个唯一的「认领槽」，利用 Redis 的 `SETNX` 原子性抢占：

抢到槽的线程是第一个写入者，继续执行消息落盘

没抢到的线程直接返回槽中已有的消息，作为最终结果

认领 Key 的四段式结构：`A2A_SESSION_REPLIED + sessionId + : + fromAgentId + : + replyTo + : + payload摘要`

四段缺一不可

sessionId：区分不同会话

fromAgentId：区分不同发言 Agent

replyTo：区分对哪条消息的回复

payload 摘要：以上三者都一样但是具体内容不同也算不同（支持Agent改口）

如果同一个 Agent 对同一条请求改判（比如从「拒绝」改成「同意」），前三段完全相同。不加内容区分的话，真实的改判会被当成重复静默丢掉，造成无声的业务错误，这比放过一次重复的后果严重得多。

## Agent 收件箱

```java
public void pushInbox(String agentId, String sessionId, long seq) {
    RQueue<String> inbox=redissonClient.getQueue(inboxKey(agentId));
    inbox.add(sessionId+":"+seq);
    inbox.expire(ttl());
}

public List<A2aMessage> drainInbox(String agentId, int limit) {
    RQueue<String> inbox=redissonClient.getQueue(inboxKey(agentId));
    List<A2aMessage> result=new ArrayList<>();
    for (int i = 0; i < limit; i++) {
        String pointer = inbox.poll();
        if (pointer == null) {
            break;
        }
        A2aMessage message = resolve(pointer);
        if (message != null) {
            result.add(message);
        } else {
            log.warn("[A2A] 收件箱里有一条指针解析不出消息，丢弃 | agent={} | pointer={}", agentId, pointer);
        }
    }
    return result;
}

private A2aMessage resolve(String pointer) {
    if (pointer==null) {
        return null;
    }
    int at = pointer.lastIndexOf(':');
    if (at <= 0) {
        return null;
    }
    long seq;
    try {
        seq = Long.parseLong(pointer.substring(at + 1));
    } catch (NumberFormatException e) {
        return null;
    }
    String sessionId = pointer.substring(0, at);
    for (A2aMessage m : readAfter(sessionId, seq - 1)) {
        if (m.getSeq() == seq) {
            return m;
        }
    }
    return null;
}

public long inboxSize(String agentId) {
    return redissonClient.getQueue(inboxKey(agentId)).size();
}
```

解耦消息投递与消费，每个 Agent 有独立的投递队列，存的是消息指针而非消息本体。

数据结构：RQueue（底层是 Redis List，用 RPUSH+LPOP 实现队列）

队列元素：指针字符串 sessionId:seq，只存位置引用，不存完整消息

pushInbox 投递指针：往目标 Agent 的队列尾部追加一条指针。

消息本体和指针是两次写入，存在 "消息在，指针不在" 的短暂窗口，但属于可接受风险。

drainInbox 批量消费

从队列头部逐个 `poll` 指针（原子消费，不会重复），最多拉取 limit 条。

指针解析成功就加入结果集

解析失败（会话过期、格式错误、序号对不上）直接丢弃
 
为什么用循环 `poll` 而不是批量弹出？

批量 `LPOP key count` 需要 Redis 6.2+，为了兼容性牺牲少量性能；且收件箱读取是非热路径，代价可接受。

resolve 指针解析

拆分 `sessionId:seq`，调用 `readAfter` 定位到具体消息。
