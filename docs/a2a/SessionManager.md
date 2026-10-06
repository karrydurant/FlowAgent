# SessionManager

## 定位

A2A 协议的会话核心服务，负责创建会话、消息投递、会话生命周期管理、成员准入、跨实例唤醒、回复去重

# 整体角色

SessionStore：Redis 存储层，会话、消息、收件箱、去重锁都在这里

AgentRegistryService：Agent 注册中心，查询 Agent 卡片、能力、端点、是否接收委派

PeerDispatcher：本地 Agent 消息执行派发器

A2aSignature：A2A HTTP 唤醒请求签名，保证跨实例调用安全

HttpClient：JDK 原生 HTTP 客户端，用来调用其它实例的 /wake 唤醒接口

# 设计思想

1.会话 open：先校验全部成员，全部合法才创建会话

```java
@Service
@Slf4j
public class A2ASessionService {
    private final AgentRegistryService agentRegistryService;
    private final String redisSessionKeyPrefix;

    public A2ASessionService(AgentRegistryService agentRegistryService,
                             @Value("${flowagent.a2a.session.redis.prefix}") String redisSessionKeyPrefix) {
        this.agentRegistryService = agentRegistryService;
        this.redisSessionKeyPrefix = redisSessionKeyPrefix;
    }

    /**
     * 打开会话：全员前置校验，全部合法才创建session
     */
    
}
```

校验每个成员要满足 3 条：

·注册中心能查到 Agent

·`acceptsDelegation=true`，允许被委派、接收 A2A 消息

·Agent 卡片配置了 a2aEndpoint 通信地址

特殊角色：LOCAL_CONVENER_AGENT_ID=a2a-convener

这个召集人不是注册中心里的 Agent，代表外部调用方（工作流节点/用户），不需要去注册中心校验，端点直接是当前服务`localBaseUrl`，角色固定 CONVENER。

2.消息投递 post：消息先写入共享 Redis，再唤醒接收方

Redis：永久会话日志，所有消息存在这里，所有实例共享读取

/wake HTTP请求：只是通知，告诉远端 Agent，你来读新消息，不再重复传递消息体

远端收到 wake 通知后，自己从 Redis 会话里读取消息，不是从 HTTP body 拿消息。所以唤醒失败（网络、对方挂了）只会抛`DELIVER_FAILED(502)`，消息已经持久化，不会丢。

投递分为两条路径：

·接收方是当前实例本地 Agent：不走 HTTP，直接调用`PeerDispatcher.dispatch`丢进线程池后台执行，跳过网络唤醒

·接收方在别的服务实例：发 HTTP POST `{agentEndpoint}/sessions/{sessionId}/wake`，带上签名，唤醒远端

3.两种消息消费模型：

·会话历史消息 messages(sessionId, afterSeq)

只读，可重复拉取。读取会话全量历史，用于复盘、查询会话记录，多次读取结果不变。

·Agent 收件箱 inbox(agentId, limit)

消费即删除（drain取出），一次性读取

只放 INFORM / PROPOSE / VOTE 三类消息。

用途：Agent 模型读取待处理的通知类消息；读走之后，下次再取收件箱就看不到这条了，防止反复喂给大模型重复内容。

REQUEST / SOLICIT / REPLY 不进收件箱：

REQUEST/SOLICIT：属于可执行任务，收到 wake 直接触发 Agent 执行，不需要进收件箱

REPLY：是应答消息，由发起方主动轮询读取，不走收件箱

4.回复去重机制（写入层防止重复投递）

针对 REPLY/PROPOSE 这两类应答消息做去重。针对的是网络重试，同一条回复多次调用 post，避免重复执行应答逻辑

·store.claimReply(message)：Redis 分布式锁抢占这条 reply 槽位，抢到才允许写入；如果已经存在，直接返回已存在的旧消息，不写入，不投递

·降级策略：Redis异常时 fail-open（放行），不阻断业务；只统计失败指标 replyGuardFailures，不抛异常

5.会话生命周期

6.错误码设计（业务语义和 HTTP 状态绑定）
| 错误码 | HTTP | 含义 |
| ---- | ---- | ---- |
| AGENT_UNAVAILABLE | 404 |	Agent 不存在 / 心跳过期下线 |
| AGENT_NOT_ACCEPTING_DELEGATION | 409 | Agent 拒绝委派 |
| AGENT_ENDPOINT_MISSING | 422	Agent 没有配置通信地址
SESSION_DEPTH_EXCEEDED	422	成员数量超过上限
SESSION_NOT_FOUND	404	会话不存在 / 过期
SESSION_CLOSED	409	会话关闭，禁止投递消息
SESSION_LOOP	409	消息发给自己，自环保护
NOT_A_MEMBER	403	发送 / 接收方不在会话成员列表
MESSAGE_BUDGET_EXCEEDED	429	会话消息数量超限，防止无限对话
DELIVER_FAILED	502	唤醒远端 Agent 网络失败（消息已落盘）
