# ReActAgent

## 整体结构

```mermaid
graph TD
    subgraph ReActAgent["ReActAgent.java（1200+行）"]
        subgraph 字段依赖
            A1["ChatClient<br/>调LLM的客户端"]
            A2["ToolBridge<br/>工具桥（MCP + A2A）"]
            A3["AgentInbox<br/>A2A收件箱"]
            A4["LlmCallProperties<br/>超时/重试配置"]
        end

        subgraph 主入口
            B["execute() 方法<br/>4个重载，最后一个是真实现"]
        end

        subgraph 核心循环["核心 for 循环（maxIterations 次）"]
            C1["① 中断检查"]
            C2["② 收件箱注入"]
            C3["③ 成本预检"]
            C4["④ buildPrompt()<br/>拼提示词"]
            C5["⑤ 调 LLM（流式）"]
            C6["⑥ 记 token 用量"]
            C7["⑦ 成本闸：超预算收紧历史"]
            C8["⑧ parseReActOutput()<br/>解析输出"]
            C9{"是 Final Answer?"}
            C10["⑨ 执行工具调用<br/>重复检测 + 白名单校验"]
        end

        subgraph 辅助方法
            D1["buildFullSystemPrompt()<br/>拼系统提示词"]
            D2["renderHistory()<br/>渲染历史Observation"]
            D3["injectInbox()<br/>收件箱取消息塞历史"]
            D4["recallSafely()<br/>读会话记忆（fail-open）"]
        end
    end

    B --> C1
    C1 --> C2
    C2 --> C3
    C3 --> C4
    C4 --> C5
    C5 --> C6
    C6 --> C7
    C7 --> C8
    C8 --> C9
    C9 -- 是 --> E["返回结果"]
    C9 -- 否 --> C10
    C10 --> C1

    C4 --> D1
    C4 --> D2
    C2 --> D3
    C4 --> D4
```

## 核心主循环流程

```mermaid
flowchart TD
    Start(["开始 execute()"]) --> Loop{"还有剩余步数?"}

    Loop -- 否 --> MaxSteps["步数耗尽<br/>返回失败结果"]

    Loop -- 是 --> S1["① 检查中断<br/>被取消就退出"]
    S1 -- 中断退出 --> Interrupted["返回 interrupted"]

    S1 --> S2["② 收件箱注入<br/>把别人发我的消息塞进历史"]
    S2 --> S3["③ 成本预检<br/>预算花完就不发这次调用"]
    S3 -- 超预算 --> BudgetFail["返回预算耗尽"]

    S3 --> S4["④ 拼提示词<br/>System + 记忆 + 历史 + 当前问题"]
    S4 --> S5["⑤ 调 LLM（流式）"]
    S5 --> S6["⑥ 记录 token 用量"]
    S6 --> S7["⑦ 成本闸<br/>超预算就收紧历史"]
    S7 --> S8["⑧ 解析 LLM 输出"]

    S8 --> Type{"输出类型?"}

    Type -- "Final Answer" --> Success(["成功返回最终答案"])
    Type -- "Thought Only" --> Next["下一轮"]

    Type -- "Action 工具调用" --> Check{"校验通过?"}
    Check -- "重复调用 / 不在白名单" --> BadObs["observation = 错误提示<br/>（引导模型直接出结论）"]
    Check -- "正常" --> RunTool["真正执行工具"]
    RunTool --> Obs["observation = 工具结果"]

    BadObs --> Next
    Obs --> Next
    Next --> Loop
```

## ReAct 主循环调度 -- execute()

```java
public ReActResult execute(String systemPrompt, String userQuery, Map<String, Object> context, int maxIterations, ReActCallbacks callbacks, Set<String> allowedTools, HistoryPolicy historyPolicy, AgentMemory memory) {
    Instant startTime=Instant.now();
    List<ReActStep> steps=new ArrayList<>();
    HistoryPolicy policy=historyPolicy != null ? historyPolicy : HistoryPolicy.FULL;
    List<AgentMemory.Turn> memoryTurns=recallSafely(memory);

    log.info("[ReAct] starting | maxIterations={} | memoryTurns={} | query={}", maxIterations, memoryTurns.size(), truncate(userQuery, 100));
    try {
        for (int iteration = 0; iteration < maxIterations; iteration++) {
            if (Thread.currentThread().isInterrupted()) {
                log.warn("[ReAct] interrupted by engine | iterations={} | steps={}",
                        iteration, steps.size());
                return interruptedResult(steps, startTime, callbacks);
            }

            log..debug("[ReAct] iteration {}/{}", iteration + 1, maxIterations);
            // 收件箱：把别人跟我说过、而我没看见的消息补进历史
            // 放在这一轮 buildPrompt 之前，下一段的渲染就自然带上了 —— 不需要
            // 另建一条通道，也不需要动循环本身。见 AgentInbox 的 javadoc。
            if (injectInbox(steps, context, iteration)) {
                return interruptedResult(steps, startTime, callbacks);
            }
            // 前置成本闸
            if (callbacks!=null && !callbacks.beforeLlmCall(iteration)) {
                log.warn("[ReAct] 成本预算已耗尽，终止推理 | iteration={} | steps={}",
                        iteration, steps.size());
                return ReActResult.costBudgetExhausted(steps,
                        Duration.between(startTime, Instant.now()).toMillis());
            }
            // 构建 prompt（System + 会话记忆 + History + 当前 Query）
            Prompt prompt = buildPrompt(systemPrompt, userQuery, steps,
                    iteration == 0, allowedTools, policy, memoryTurns);
            //调用 LLM（流式）
            Instant llmStart=Instant.now();
            ChatResponse response=LlmStreams.await(
                    () -> chatClient.prompt(prompt).stream().chatResponse(),
                    llmCalls, "ReActAgent");
            String llmOutput=response.getResult().getOutput().getText();
            long llmDurationMs = Duration.between(llmStart, Instant.now()).toMillis();

            //提取 tokn 用量与模型名（供成本核算 / 全链路追踪使用）
            ChatResponseMetadata meta = response.getMetadata();
            Usage usage = meta != null ? meta.getUsage() : null;
            int promptTokens = usage != null && usage.getPromptTokens() != null
                    ? usage.getPromptTokens() : 0;
            int completionTokens = usage != null && usage.getCompletionTokens() != null
                    ? usage.getCompletionTokens() : 0;
            String model = meta != null && meta.getModel() != null ? meta.getModel() : "unknown";

            log.debug("[ReAct] LLM output ({}ms, {}/{} tokens, {}):\n{}",
                    llmDurationMs, promptTokens, completionTokens, model, truncate(llmOutput, 500));
            //回调：记录 LLM 调用（含 token 用量与模型）
            if (callbacks != null) {
                callbacks.onLlmCall(llmOutput, llmDurationMs, promptTokens, completionTokens, model);
            }
            //成本闸
            if (policy.isBudgetExceeded(promptTokens)) {
                if (policy.atFloor()) {
                    log.warn("[ReAct] prompt token 预算耗尽且历史已收紧到下限，终止推理"
                                    + " | promptTokens={} | budget={} | policy={} | steps={}",
                            promptTokens, policy.getPromptTokenBudget(), policy, steps.size());
                    if (callbacks != null) {
                        callbacks.onBudgetExhausted(promptTokens, policy);
                    }
                    // 刻意不调 onComplete：预算耗尽是失败终态，发 AGENT_COMPLETE 是在撒谎
                    return ReActResult.budgetExhausted(steps,
                            Duration.between(startTime, Instant.now()).toMillis());
                }

                HistoryPolicy tightened = policy.escalate();
                log.warn("[ReAct] prompt tokens {} 超出预算 {}，收紧历史策略"
                                + " | window {}→{} | maxObsChars {}→{}",
                        promptTokens, policy.getPromptTokenBudget(),
                        policy.getWindowSize(), tightened.getWindowSize(),
                        policy.getMaxObservationChars(), tightened.getMaxObservationChars());
                if (callbacks != null) {
                    callbacks.onHistoryEscalated(promptTokens, policy, tightened);
                }
                policy = tightened;
            }
        }
    }
}
```
