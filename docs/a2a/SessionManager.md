# SessionManager

## 定位

是 FlowAgent 中 A2A 会话体系的会话生命周期与消息投递管理器

它的核心职责是：

1.负责协作会话的创建、准入校验与全生命周期管理

2.负责会话消息的持久化落盘、成员分发与跨实例唤醒

3.维护会话成员准入、消息预算、回复去重等核心管控规则

4.提供会话历史查询、收件箱消费、状态追踪等标准化读取能力

## 设计目标

1.一致性：分布式场景下会话状态、消息顺序、业务规则全局统一

2.可用性：非核心链路故障不阻塞主流程，单点故障不扩散，降级可运行

3.可观测性：关键路径全埋点，故障类型可分类，问题现场可追溯

## 会话一致性保障

前置准入校验：全通过才建会话

```java
// 先逐个校验所有成员合法性
for (String agentId : ordered) {
    members.add(localConvener && agentId.equals(initiator)
            ? localMember(agentId)
            : resolveMember(agentId, roles));
}
// 全部校验通过后，才构建会话并写入Redis
A2aSession session = A2aSession.builder()...build();
store.save(session);
```

