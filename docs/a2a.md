# 首先要明白一次Agent协作里都有谁

### 先走一个简单例子，有一个故障诊断工作流跑到某个节点，要问另一个 Agent 一件事

##### 第一步：开会话

工作流节点调网关注册一场会话。网关拿着名单逐个查注册中心：这个 Agent 在线吗? 它登记了投递地址吗？任何一个人查不到，整场会话直接拒绝，一个请求都不发。

##### 第二步：会话和成员落盘

生成的会话对象里，除了名单，还额外记了每个人的投递地址快照和本次角色。地址为什么快照，等下讲。

##### 第三步，投一条 REQUEST

投一条 REQUEST。 落进 Redis 的消息长[这样](docs/p1.txt)

##### 第四步，叫醒对方

消息落盘之后，再给对方的实例发一句 “你有新消息”。注意这里是两件事：写一次，叫一次。为什么不是顺手在"叫醒"里把消息也写一遍，这是第 2 讲的核心。

##### 第五步，对方来读

log-agent 收到叫醒，带着自己的游标去读会话日志，要求“只给我序号大于 N 的”。它读到 seq=1 这条，真的跑一遍自己的 Agent 循环。

##### 第六步，回报

log-agent 拿到答案，投一条 REPLY 回去，replyTo 填的是第一步那条的 messageId (m-8f21)。

##### 第七步，配对

发起方一直在同一场会话里等，等的条件不是"来了一条消息"，而是"来了一条 replyTo 等于我那条 messageId 的 REPLY"。对上了，把答案拿走继续往下跑。

##### 第八步，收尾

会话转成已关闭，日志留在那里。事后按会话 id 就能把整个过程读回来——这是唯一的事后复盘入口。

### 抽象成骨架

##### 把上面 8 步里出现过的东西归类，分为 3 类

会话（这次协作的容器）

成员表（谁参加，怎么找他，他这次是什么角色，例如
| repair-agent | 快照地址 | 召集人）

消息日志（谁对谁说了什么，按会话内序号排，例如
| seq=1 | REQUEST | repair-->log |）

外加一个不在这三张表里、但贯穿始终的东西：Redis。会话、成员、日志都存在那儿，而且会话日志是唯一真相，收件箱之类的结构只存指针、不存副本，就是为了不出现第二个真相。

还有一句要记住的总纲，后面每一讲都在解释它：

委派、协商、主动通信这三种看起来不同的协作，在这个项目里是同一条通道的三种用法。

委派 = 发一条 REQUEST 等 REPLY；协商 = 多轮 PROPOSE 收敛；主动通信 = 发一条 INFORM 不等回复。区别只在"谁发起、等不等回复"。

好处不是省代码，是鉴权、防环、留痕各只有一份。这个项目在被改造之前，委派和协商各走各的路，结果是两条路各有一份超时、各有一份失败处理——第 5 讲会看到那份超时后来怎么变成事故。

### 三个结构

##### A2aMessage

| 字段 | 类型 | 含义 |
| ---- | ---- | ---- |
| message | String | 消息唯一标识，服务端生成 |
| sessionId | String | 所属会话 |
| seq | long | 会话内单调递增序号，存储层分配，用作增量拉取的游标 |
| fromAgentId | String | 发送方 |
| toAgentId | String | 接收方，null 表示广播 |
| kind | enum | 消息种类，同时是 payload 的结构选择器 |
| payload | Map | 消息体，结构由 kind 决定 |
| replyTo | String | 本条回复的是哪条消息（填对方的 messageId）|
| importance | enum | NORMAL/CRITICAL |
| createdAt | Instant | 投递时间 |

**kind**

有六个值，各自的约定

| kind | 谁发 | payload | 会不会触发对方干活 |
| ---- | ---- | ---- | ---- |
| REQUEST | 委派方 | {task, depth?} | 会 |
| REPLY | 被委派方 | {success, answer} 或 {success:false, error}，必须带 replyTo| 不会 |
| SOLICIT | 协商召集人 | {decision, confidence, reason}，必须带 replyTo | 不会 |
| PROPOSE | 协商参与者 | {vote} | 不会 |
| INFORM | 任何成员 | 自由结构 | 不会 |

importance 这个字段容易被误读成"消息优先级"。它其实只服务一件事：Agent 的历史窗口在上下文变长时会收紧，收紧时默认只留最后 4 步，一次多 Agent 协作很容易超过 4 步，"先别动生产库"这类早期关键约束会被悄悄裁掉。所以消息可以自报关键，裁剪时关键消息不被丢——这是它存在的唯一理由。

##### A2aSession 

| 字段 | 类型 | 含义 |
| ---- | ---- | ---- | 
| sessionId | String | 会话唯一标识 |
| topic | String | 这次协作要解决的事；协商场景下就是议题 |
| initiator | String | 发起方 |
| members | List<Member> | 成员表，含发起方自己 |
| status | enum | OPEN/CLOSED/EXPIRED |
| messageCount | int | 已投递条数，用于消息预算判定 |
| fromRunId | String | 由哪次工作流运行发起；null=不是工作流发起的（用户在会话台当场开的） |
| fromNodeId | String | 由哪个节点发起；null 含义同上 |
| createdAt | Instant | 创建时间 |
| updatedAt | Instant | 最后一次状态变更时间 |

##### Member

| 字段 | 类型 | 含义 |
| ---- | ---- | ---- |
| agentId | String | 成员标识 |
| endpoint | String | 开会话时从注册表快照下来的投递地址 |
| role | enum | 本次协作里的角色 |
| joinedAt | Instant | 加入时间 |

**role**四个值
| role | 含义 |
| ---- | ---- |
| PARTICIPANT | 普通参与者，投一票，票数参与聚合 |
| VETO | 持否决权，投反对即终止讨论，不看票数 |
| SUPERVISOR | 主管，不投常规票，各方意见齐了之后被单独征询裁决 |
| CONVENER | 召集人，不表态，负责投出每一轮的议题 |

### 表后面的隐含规则





