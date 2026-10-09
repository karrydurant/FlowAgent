# WorkflowEngine

## 定位

它负责一件事：拿到一张 DAG 工作流定义 + 输入参数，怎么一层一层把节点跑完，中间出问题了怎么重试、降级、挂起、断点续跑

## 核心流程

```mermaid
flowchart TD
    %% ==================== 对外入口 ====================
    Start([开始]) --> EntryChoice{入口类型}

    EntryChoice -->|同步执行新流| ExecuteNew["execute(def, input)"]
    EntryChoice -->|异步提交新流| SubmitNew["submit(def, input)"]
    EntryChoice -->|同步断点恢复| ResumeNew["resume(def, runId)"]
    EntryChoice -->|异步断点恢复| ResumeAsyncNew["resumeAsync(def, runId)"]
    EntryChoice -->|审批结果唤醒| ApprovalWake["resumeAfterApproval(def, runId, nodeId, verdict, comment)"]

    %% 新工作流路径
    ExecuteNew --> CreateRun["createRun(def, input)<br/>校验参数 · DAG验证 · 创建Run"]
    SubmitNew --> CreateRun
    SubmitNew -.后台异步.-> WorkflowPool[workflowExecutor 线程池]
    WorkflowPool --> ExecInternal

    %% 断点恢复路径
    ResumeNew --> PrepareResume["prepareResume(def, runId)<br/>加载快照 · 指纹校验 · 清理残留"]
    ResumeAsyncNew --> PrepareResume
    ResumeAsyncNew -.后台异步.-> WorkflowPool

    %% 审批唤醒路径
    ApprovalWake --> ApprovalIdCheck{幂等门:<br/>确实停在该节点<br/>上等审批?}
    ApprovalIdCheck -->|否| WakeDrop[丢弃本次唤醒<br/>直接返回]
    ApprovalIdCheck -->|是| VerdictChoice{审批结论?}

    VerdictChoice -->|APPROVED| SettleApproved["settleApproved()<br/>审批节点记完成 · 写输出"]
    VerdictChoice -->|REJECTED / TIMED_OUT / CANCELLED| SettleRejected["settleRejectedOrTimedOut()<br/>等价节点失败 · 走降级"]

    SettleApproved --> PrepareResume
    SettleRejected --> FallbackStop{降级结果是<br/>PAUSE / FAIL?}
    FallbackStop -->|是| StopHere[停在这里<br/>不再启动执行]
    FallbackStop -->|否 → SKIP / FALLBACK_VALUE| PrepareResume

    CreateRun --> ExecInternal

    %% ==================== 核心执行主流程 ====================
    ExecInternal["executeInternal(def, run)<br/>🔥 核心方法"] --> LeaseTry{抢执行租约}
    LeaseTry -->|失败/异常| RejectFail["rejectRun()<br/>落 FAILED · 直接返回"]
    LeaseTry -->|成功| MarkRunning["状态置 RUNNING<br/>登记 inflightIndex · 开 tracer"]

    MarkRunning --> TopoSort["TopologicalSorter.sort(def)<br/>拓扑分层 → layers"]
    TopoSort --> LayerLoop{遍历每一层}

    LayerLoop --> LayerFilter["filterByConditions()<br/>条件过滤 · 决定哪些节点该跑"]
    LayerFilter --> ActiveEmpty{activeNodes 为空?}
    ActiveEmpty -->|是| NextLayer
    ActiveEmpty -->|否| DoLayer["executeLayer()<br/>本层并行执行"]

    DoLayer --> LayerDone{整层跑完后<br/>状态还是 RUNNING?}
    LayerDone -->|否 → 被暂停/失败了| BreakLoop[跳出循环]
    LayerDone -->|是| NextLayer[下一层]
    NextLayer --> LayerLoop

    BreakLoop --> FinalJudge
    LayerLoop -->|所有层跑完| FinalJudge{最终状态判定}

    FinalJudge -->|全局超时| GlobalTimeout["markGlobalTimeout()<br/>状态置 FAILED"]
    FinalJudge -->|failedNodeIds 为空| SuccessDone["COMPLETED<br/>写最终输出 · 删除 checkpoint"]
    FinalJudge -->|failedNodeIds 非空| PartialDone["PARTIAL_SUCCESS<br/>补存 checkpoint · 保留可恢复"]

    %% ==================== executeLayer 内部 ====================
    DoLayer --> NodeCount{本层节点数?}
    NodeCount -->|=1| RunSingle["直接 executeSingleNode()<br/>省线程池开销"]
    NodeCount -->|>1| RunParallel["并行执行<br/>Semaphore(maxConcurrency)<br/>控制并发上限"]

    RunParallel --> WaitAll["all.get(剩余预算)<br/>等整层完成"]
    WaitAll -->|超时| LayerTimeout[cancel 所有 future<br/>触发全局超时]
    WaitAll -->|全部完成| LayerComplete[本层结束]
    LayerTimeout --> LayerComplete

    RunSingle --> LayerComplete
    LayerComplete --> LayerDone

    %% ==================== executeSingleNode 内部 ====================
    RunSingle --> ExecNode
    RunParallel --> ExecNode

    ExecNode["executeSingleNode(def, run, nodeId, deadline)"] --> RetryLoop{重试循环<br/>attempt ≤ maxRetries}
    RetryLoop -->|非首次重试| SleepRetry[睡 retryDelaySeconds]
    SleepRetry --> GetExec
    RetryLoop -->|首次| GetExec["nodeExecutorFactory.get(type)<br/>拿对应执行器"]

    GetExec --> BuildIn["buildNodeInput()<br/>拼: 全局变量 + 前驱输出"]
    BuildIn --> SubmitNode["提交到 nodeCallExecutor<br/>线程池"]
    SubmitNode --> WaitNode["future.get(nodeTimeout)<br/>超时钳制到全局剩余预算"]

    WaitNode -->|成功| CommitOK["commitNodeSuccess()<br/>加锁 · 写变量/完成集合 · 存 checkpoint"]
    CommitOK --> NodeFinish[节点完成]

    WaitNode -->|超时 / 执行异常| ErrCheck{是挂起异常?}
    ErrCheck -->|是 → WorkflowSuspendedException| ThrowSuspend[原样向上抛<br/>不重试]
    ThrowSuspend --> InternalCatch

    ErrCheck -->|普通错误| RetryInc[attempt++ · 记 lastError]
    RetryInc --> RetryLoop

    RetryLoop -->|重试耗尽| SettleFail["settleNodeFailure()<br/>记失败集合 · 存结果"]
    SettleFail --> DoFallback["handleFallback()<br/>降级策略"]

    DoFallback --> FallbackKind{降级策略}
    FallbackKind -->|SKIP| FbSkip["不记 completed<br/>下游被拦住 · 兄弟分支照跑"]
    FallbackKind -->|FALLBACK_VALUE| FbValue["记 completed<br/>写兜底输出 · 下游照跑"]
    FallbackKind -->|PAUSE| FbPause["状态置 PAUSED<br/>存 checkpoint"]
    FallbackKind -->|FAIL| FbFail["状态置 FAILED<br/>存 checkpoint"]

    FbSkip --> NodeFinish
    FbValue --> NodeFinish
    FbPause --> NodeFinish
    FbFail --> NodeFinish

    NodeFinish --> LayerComplete

    %% ==================== catch 和 finally ====================
    InternalCatch["executeInternal catch 块"] --> SuspendCheck{找到挂起信号?}
    SuspendCheck -->|是| HandleSuspend["handleSuspend()<br/>状态置 WAITING_APPROVAL<br/>记挂起节点 · 存 checkpoint"]
    SuspendCheck -->|否| InternalError["状态置 FAILED<br/>记错误信息"]

    HandleSuspend --> FinallyBlock
    InternalError --> FinallyBlock

    GlobalTimeout --> FinallyBlock
    SuccessDone --> FinallyBlock
    PartialDone --> FinallyBlock
    RejectFail --> End
    StopHere --> End
    WakeDrop --> End

    FinallyBlock["finally 块<br/>放租约 · 移出 runningWorkflows<br/>终态摘 inflight 索引<br/>存入 finishedWorkflows<br/>结束 tracer span"] --> End([结束])

    %% ==================== 样式 ====================
    classDef entry fill:#e3f2fd,stroke:#1976d2,stroke-width:1.5px
    classDef core fill:#fff3e0,stroke:#e65100,stroke-width:2.5px
    classDef decision fill:#f3e5f5,stroke:#7b1fa2,stroke-width:1.5px
    classDef terminal fill:#ffebee,stroke:#c62828,stroke-width:2px
    classDef success fill:#e8f5e9,stroke:#388e3c,stroke-width:1.5px

    class EntryChoice,LeaseTry,ActiveEmpty,LayerDone,NextLayer,FinalJudge,NodeCount,RetryLoop,ErrCheck,FallbackKind,VerdictChoice,ApprovalIdCheck,FallbackStop,SuspendCheck decision
    class ExecInternal,DoLayer,ExecNode,CommitOK,PrepareResume core
    class SuccessDone,NodeFinish,LayerComplete,CommitOK success
    class RejectFail,GlobalTimeout,InternalError,HandleSuspend,PartialDone,StopHere,WakeDrop terminal
```

## 一层怎么并行跑 -- executeLayer()

```java
private void executeLayer(WorkflowDefinition def, WorkflowRun run, List<String> nodeIds, Deadline deadline) {
    if (deadline.expired()) {
        markGlobalTimeout(def, run);
        return;
    }

    if (nodeIds.size()==1) {
        executeSingleNode(def, run, nodeIds.get(0), deadline);
        return;
    }

    // 并发闸门：同一时刻最多 maxConcurrency 个节点执行
    // 层内节点数可能远大于该值，超出的在此排队
    int maxConcurrency=Math.max(1, def.getConfig().getMaxConcurrency());
    Semaphore permits=new Semaphore(maxConcurrency);
    log.debug("[Executor] layer concurrency gate | runId={} | nodes={} | maxConcurrency={}", run.getRunId(), nodeIds.size(), maxConcurrency);
    // Void表示任务返回结果
    List<CompletableFuture<Void>> futures=new ArrayList<>();
    for (String nodeId : nodeIds) {
        // runAsync：不传 Executor，使用默认线程池，这里使用的是 layerExecutor 线程池
        // runAsync(Runnable, Executor)，第一个参数传的是 Runnable 实例
        futures.add(CompletableFuture.runAsync(
                // wrap() 捕获当前主线程的 ThreadLocal，当任务在 layerExecutor 的子线程执行时，把捕获到的上下文重新塞到子线程的 ThreadLocal。
                FlowAgentContext.wrap(() -> {
                    try {
                        permits.acquire();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(ie);
                    }
                    try {
                        executeSingleNode();
                    } finally {
                        permits.release();
                    }
                }),
                layerExecutor));
    }

    // 先转成数组，符合参数规范
    // 采用 allOf 策略
    // 这行代码不是阻塞的，allOf 只是注册组合监听，立刻返回，主线程继续往下跑
    // allOf 是异步任务组合器，它本身也是一个异步操作，所以它返回的结果也必须是 CompletableFuture
    CompletableFuture<Void> all=CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    try {
        if (deadline.unlimited()) {
            all.join();
        } else {
            //带超时阻塞等待这批节点任务全部完成
            all.get(Math.max(1, deadline.remainingSeconds()), TimeUnit.SECONDS);
        }
    } catch (TimeoutException te) {
        //全局超时：放弃等待本层，把工作流置为失败
        futures.forEach(f -> f.cancel(true));
        markGlobalTimeout(def, run);
    } catch (InterruptedException ie) {
        futures.forEach(f -> f.cancel(true));
        Thread.currentThread().interrupt();
        throw new CompletionException(ie);
    } catch (ExecutionException ee) {
        // 单节点的异常已在 executeSingleNode 内部消化（重试/降级），
        // 能冒到这里的是编排层自身的异常，不应被吞掉
        throw new CompletionException(ee.getCause() == null ? ee : ee.getCause());
    }
}
```

## 单个节点的完整生命周期 -- executeSingleNode()

```java
public void executeSingleNode(WorkflowDefinition def, WorkflowRun run, String nodeId, Deadline deadline) {
    WorkflowNode node=def.getNode(nodeId);
    WorkflowRun.NodeExecutionResult result=WorkflowRun.NodeExecutionResult.builder().
            .nodeId(nodeId)
            .nodeName(node.getName())
            .status(WorkflowRun.NodeExecutionResult.NodeStatus.RUNNING)
            .startedAt(Instant.now())
            .build();
    run.getActiveNodeIds().add(nodeId);
    run.addTimelineEvent("NODE_START", nodeId, "开始执行: " + node.getName());
    log.info("[Executor] node start | runId={} | node={} | type={}",
            run.getRunId(), nodeId, node.getNodeType());

    int attempt=0;
    Exception lastError=null;

    while (attempt <= node.getMaxRetries()) {
        try {
            if (attempt>0) {
                log.info("[Executor] node retry {}/{} | runId={} | node={}",
                        attempt, node.getMaxRetries(), run.getRunId(), nodeId);
                Thread.sleep(node.getRetryDelaySeconds() * 1000L);
            }

            // 获取节点执行器
            NodeExecutor executor=nodeExecutorFactory.get(node.getNodeType());
            Map<String, Object> nodeOutput;
            try {
                nodeOutput=future.get(nodeTimeout, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);
                throw new FlowAgentException("NODE_TIMEOUT",
                        "节点 " + node.getName() + " 执行超时 (" + nodeTimeout + "s)");
            } catch (ExecutionException e) {
                Throwable cause
            }
        }
    }
}
```

