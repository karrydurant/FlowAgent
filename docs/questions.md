# "Agent互相委派"

## 委派的判断逻辑（什么时候委派）写在哪里？prompt 里、代码规则里，还是 LLM 自主判断？

在本项目中 "智能体A" 委派 "智能体B" 实际上就是 A 把 B 当成工具了（PeerAgentToolBridge.java 中的 listTools() 方法会[把智能体 "渲染" 成工具](docs/agent-to-tool.md)）。

模型眼里的工具表长这样：

query_database(...)  <- MCP 工具

call_agent_docs(...)  <- 对等体，被渲染成工具

模型要委派，就是调一个工具。跟调 query_database 没有任何区别

判断"什么时候该委派"全靠模型自己，但是 "能委派什么"和"委派不成怎么办"全在代码：

1.谁能被看到

listTools() 只列 acceptsDelegation=true 的；allowedTools 白名单默认关闭 -- 白名单就是拓扑。 

模型看不到的，它就调不到

2.能不能委派

三个前置检查：身份缺失，目标是自己，深度超限都拒绝

模型想调，代码可以不让

3.不成功怎么办

等待循环 + replyTo 配对 + deadline

模型不需要管超时和错配

## 把 "什么时候该委派" 写成代码规则会怎样

## A 把任务交给 B 的时候，具体传了哪些信息？是纯自然语言，还是结构化字段？

先看模型那一侧看到的工具签名：参数只有 task，描述的是："要交给这个 Agent 完成的任务"，要求用自然语言写

再看实际发出去的消息体

formAgentId: "docs-agent"

toAgentId: "security-agent"

kind: "REQUEST"

payload: {task:"帮我看看这段 SQL 有没有锁表风险……", depth=1}

| 消息里的层 | 装什么 | 谁来读它 |
| ---- | ---- | ---- |
| 信封 | fromAgentId、toAgentId、kind、replyTo、seq | 代码 —— 路由、配对、防重复执行全靠它 |
| 内容 | payload.task，一整段自然语言 | 模型 —— 只有模型能懂"注意别锁表" |

## B 答完之后，A 怎么认出这条回复是回给自己那条请求的？

配对键是 replyTo。

```java
long startSeq=0L;
Instant deadline=Instant.now().plusSeconds(callTimeoutSeconds);
while (true) {
    ReadResult read = readAfter(sessionId, startSeq);
    startSeq = read.maxSeq();
    for (Map<String, Object> message : read.messages()) {
        if (isReplyTo(message, messageId)) { return payload; }
    }
}
```

为什么是从 0 开始读，而不是从"现在最新"开始读？ 从最新读看着更省，但会漏掉一个窗口里的回复：
消息投出去之后、第一次轮询之前，对方可能已经答完了（本实例成员走 dispatch，连网络都不用）。从 0 读就不会漏。

配对判据：kind == "REPLY" && replyTo == 我发出去的那条 messageId。

而"谁答的"由另一条规则保证 —— 执行类消息必然带 toAgentId 广播的那些在 PeerDispatcher 就被拦下了。

## B 执行失败、B 一直不回、A自己被中断 -- A 分别得到什么？

| 出什么状况 | A得到什么 |
| ---- | ---- |
| B 跑失败了 | 不抛异常 —— 收到一条 {success: false, error: ...} 的 REPLY |
| B 一直不回 | 等满 callTimeoutSeconds 后抛 DELIVER_TIMEOUT 504，话是"消息已投递，但对方未在时限内作答" |
| A 自己被中断（引擎节点超时/取消） | 抛 A2A_PEER_INTERRUPTED 500，重抛前重新置位中断位 |

非中断失败原样抛出 → ReActAgent 把它转成一条 Error Observation，模型看得见、可以自己改道。这里刻意不降级成"空回复"：一次失败的委派被读成"对方回了空内容"，模型会顺着一个不存在的事实往下编。

失败有三种处理方式，这个项目选了最贵的那种 —— 不吞、不猜、原样交回给模型。

吞掉（返回空）→ 模型顺着虚构的事实往下编。

传输层自动重试 → 副作用可能重复，而且模型不知道发生过重试。

交回模型（这个项目的选择）→ 模型看见"对方失败了"，自己决定换人、降级还是放弃。

贵在哪：每次失败多花一个 ReAct 回合。为什么值：因为"该不该重试"取决于为什么失败 —— 对方超时可能是在忙（该等），也可能是这任务它做不了（该换人）。
传输层分不出这两种，而 error 文本就在 payload 里，模型读得到。

这也顺带回答了：这个项目不做自动重委派。子 Agent 崩了，僵局交回发起方 —— 重委派的决定在模型手里，不在框架里。

## 如果 A 重试，会不会把有副作用的工具重复执行一遍？

会。先看现有的门禁：trySet("handled:" + messageId, ttl)，原子单命令，在提交线程池之前同步执行。
它挡得住：同一条消息被送达两次（wake 重试、重放、双投）。键是 messageId，标识的是"这条消息"。
它挡不住：模型层面的重试。因为模型重试会完整走一遍 callTool → 新开一个会话 → postRequest 拿到另一个 messageId → 顺利过闸 → 再执行一遍。
门禁的粒度是消息级，不是业务级。要真正挡住，需要一个标识"这件事"而不是"这条消息"的幂等键 —— 项目里现在没有。

## 要修上面那个缺口，幂等键该换成什么？为什么不能直接拿 task 文本当键？

## A 把活 交给 B 后，是 "追踪" 这个任务，还是别的什么？

A 阻塞。代价

## 任务状态到底存在哪里
