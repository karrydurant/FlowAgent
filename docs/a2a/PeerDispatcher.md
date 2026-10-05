# 整体定位

是 FlowAgent 中 A2A 会话体系的任务执行派发器。它的核心职责是：接收投递过来的会话消息，
筛选出需要目标 Agent 实际执行的消息类型，在独立线程池中调用目标 Agent 完成任务，最终将执行结果通过 HTTP 接口回写到原会话中，形成完整的 “请求 - 应答” 闭环。

# 三个核心设计目标

防循环：Agent 消息循环 + 组件依赖循环

幂等性保障：同一条消息无论被唤醒多少次，保证只执行一次

故障隔离与可观测：执行逻辑与投递逻辑解耦，异常不扩散，故障类型可被精准监控

## 防环设计

可执行消息白名单

```java
private static final Set<A2aMessage.MessageKind> EXECUTABLE_KINDS =
        EnumSet.of(A2aMessage.MessageKind.REQUEST, A2aMessage.MessageKind.SOLICIT);
```

仅 REQUEST（委派任务）、SOLICIT（征询意见） 两种消息会触发实际执行 -- 这两类都要对方 Agent 运行推理后才能给答复。

这是防循环的核心。执行完成后会回投回复消息，而回复消息不在可执行集合里，链路到此自动终止。

另外回复类型是由请求决定的：

REQUEST->REPLY

SOLICIT->PROPOSE（立场表态）

回写走 HTTP 而非直接注入


## 分布式幂等去重机制

基于 Redis 实现分布式幂等闸

### 三种状态定义

| 状态 | 含义 | 重放行为 |
| ---- | ---- | ---- |
| RUNNING | 已认领、正在执行中，未到终态 | 直接拦截 | 
| SUCCEEDED | 执行完成且回复已送达（无论业务成功失败） | 永久拒绝重放 |
| FAILED | 执行完成但回复投递失败 | 允许重投抢回、重新执行 |

### markHandled():三步原子认领逻辑

抢占 "这条消息由我接手" 的原子操作，三步设计完全规避竞态窗口：

```java
private boolean markHandled(String messageId) {
    if (messageId==null) {
        return true;
    }
    try {
        RBucket<String> guard=redissonClient.getBucket(
                CacheConstants.A2A_SESSION_HANDLED+messageId);
        if (guard.trySet(RUNNING, handledTtlSeconds, TimeUnit.SECONDS)) {
            return true;
        }
        if (guard.compareAndSet(FAILED, RUNNING)) {
            return true;
        }
        return guard.trySet(RUNNING, handledTtlSeconds, TimeUnit.SECONDS);
    } catch (Exception e) {
        long n=dedupeFailures.incrementAndGet();
        if (n==1 || n%100==0) {
            log.warn("[A2A] 幂等闸读不到，本次放行不做去重 | 累计 {} 次 | message={} | error={}",
                    n, messageId, e.toString());
        }
        return true;
    }
}
```

第一步 trySet(RUNNING):

如果键不存在（没人认领过）或已过期，直接设置为 `RUNNING` 并写入 TTL，抢到则返回 true。

用 `trySet` 而非 `get+set`：避免两步之间的竞态窗口，防止并发重放同时读到 null。

值与 TTL 一次原子写入：避免 `set+expire` 分两次命令产生的时间窗口。

第二步 compareAndSet(FAILED, RUNNING):

如果上一次执行完但回复没送达（状态为 FAILED），允许抢回重新执行。

直接使用 CAS，不用先 get 再判断，减少一次 Redis 往返，同时避免读到的值在判断时过期。

第三步 再次 trySet(RUNNING)：

只处理 “第一步返回 false、到第二步之间 TTL 恰好到期” 的极端边界场景。

整个逻辑最多两次 `trySet` + 一次 CAS，必然终止，无循环风险。

### 关键设计细节

TTL约束：`handledTtlSeconds` 必须大于 `dispatchTimeoutSeconds`（默认是 10 倍），
否则标记会在执行还没结束时过期，重放就能挤进来跑第二遍，闸门直接失效。

Fail-Open 降级策略：Redis 异常时不阻断执行，直接放行但累计计数。

如果 Redis 抖动就停止干活，会导致大量委派变成永久死消息。

配套 `dedupeFailures` 计数器：专门记录放行次数

### markSettled()：终态写入

用 compareAndSet(RUNNING, state) 而非直接 set：

引入了 FAILED 可回收机制，可能出现 "执行超时 -> 重投抢回并跑完 -> 旧执行回头写终态" 的场景，无条件 set
会覆盖新一轮的 RUNNING 状态

CAS 以 “自己还在 RUNNING” 为条件，迟到的旧执行者静默退场，不影响新执行。

不顺手刷新 TTL：沿用认领时的 TTL，避免 `compareAndSet+expire` 两条命令之间产生 
“状态已是终态、TTL 已到期” 的窗口。

Redis 异常只打 WARN 日志、不向上抛出：执行已经结束，不能因为标记失败把业务结果变成异常。

### releaseHandled()：标记释放

线程池拒绝任务时，必须删除已认领的标记，否则这条消息会被永久标记为 “已处理”，变成再也不会被执行的死消息。

## 线程池设计

```java
this.pool = new ThreadPoolExecutor(
        2, 8, 60L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(200),
        r -> {
            Thread t = new Thread(r, "a2a-peer-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        },
        new ThreadPoolExecutor.AbortPolicy());
```

池隔离：与协商场景的线程池分开。协商是 “一轮并发问 N 个 Agent 然后全部等回”，本池是 “后台把一件委派做完”；
合成一个池会导致大量协商任务挤掉委派任务。

拒绝策略为 AbortPolicy：
队列满说明并发委派已超载，如果用 `CallerRunsPolicy` 
让调用线程（发起方的 POST 请求线程）去跑 Agent，会把发起方阻塞几十秒。

有界队列 200：做流量削峰，应对瞬时突刺，不会直接拒绝。

守护线程：不阻塞 JVM 正常退出


