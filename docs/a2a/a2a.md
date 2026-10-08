# A2A

## 定位

让跑在不同进程、不同机器上的 AI Agent 互相发消息、协作干活。

## [架构图](a2a-structure.md)

## 第一层 A2aController

它是这个模块唯一对外的 HTTP 入口。外部所有请求都先进它：

前端页面发的 "开个会话"

工作流引擎节点发的 "我要委派"

其他实例发过来的 "你有新消息"（`/wake`）

它自己不做业务，只负责：解析 HTTP 请求 --> 调下面的 Service --> 包成 JSON 返回。

## 第二层 两个用例 Manager

`DelegationManager`：单向委派。
"你帮我干件事，干完把结果告诉我。"——它开个会话、投一条 REQUEST、然后死等一条 REPLY 回来。
 
`NegotiationManager`：多人协商。
"你们三个都说说自己的立场，我汇总一下。"—— 它多轮发 SOLICIT（征求意见），收 PROPOSE（各方立场），最后用 `NegotiationAggregator` 聚合成结论。

## 第三层 SessionManager

它负责 4 件事：

1.准入：开会话时挨个查成员在不在线、接不接受委派。

2.落盘：每条消息按顺序写进共享 Redis，分配一个递增序号 `seq`。

3.去重：防止同一条回复因为重试被写两遍。

4.分发：写完消息后，告诉每个接收方 "你有新消息"。

存数据找 SessionStore，执行找 PeerDispatcher

## 第四层 PeerDispatcher

SessionManager 写完消息，如果接收方是本实例的 Agent，就直接调 PeerDispatcher.dispatch()。

它负责：读那条消息 --> 看 kind 是不是 REQUEST/SOLICIT --> 是的话真去调 ReActAgent 跑一遍 --> 把结果打包成 REPLY 投回会话

它和 SessionManager 故意不互相依赖（避免循环依赖），回投结果走 HTTP 打自己的 /messages 接口。

## 第五层 SessionStore

全模块唯一碰 Redisson 的类。`SessionManager` 想存会话、追加消息、抢去重槽、推收件箱，全部通过它（Redisson）。
它不知道业务，只知道 "对象 → JSON → Redis key"。

## 数据模型

`A2aMessage`：一条消息长什么样

`A2aSession`：一个会话长什么样

`Member`：一个成员长什么样

`NegotiationAggregator`：纯算法，聚合意见用

它们没有业务方法，就是数据载体。

