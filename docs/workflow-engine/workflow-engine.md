# workflow-engine

## [架构图](detail/workflow-engine-structure.md)

## [WorkflowExecutor](detail/WorkflowExecutor.md)

整个引擎的大脑，所有调度逻辑都在这：DAG 怎么分层跑、节点怎么并发、失败怎么降级、checkpoint 什么时候存、
resume 怎么恢复

## [ReActAgent](detail/ReActAgent.md)

Agent 节点的灵魂，手写的 Thought->Action->Observation 循环

## [WorkflowRun](detail/WorkflowRun.md)

运行时状态模型--所有执行状态都存在这一个对象里：已完成节点、变量、时间线、租约锁、checkpoint 互斥锁。

##
