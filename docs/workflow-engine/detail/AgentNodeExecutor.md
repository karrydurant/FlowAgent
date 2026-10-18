# AgentNodeExecutor

```java
package com.flowagent.workflow.executor.impl;

import com.flowagent.common.artifact.ArtifactRef;
import com.flowagent.common.exception.FlowAgentException;
import com.flowagent.observe.cost.CostBudgetManager;
import com.flowagent.observe.cost.CostTracker;
import com.flowagent.observe.tracing.AgentTracer;
import com.flowagent.workflow.agent.AgentCatalog;
import com.flowagent.workflow.agent.AgentDefinition;
import com.flowagent.workflow.agent.ReActAgent;
import com.flowagent.workflow.agent.ReActAgent.HistoryPolicy;
import com.flowagent.workflow.agent.ReActAgent.ReActResult;
import com.flowagent.workflow.config.WorkflowEngineProperties;
import com.flowagent.workflow.executor.NodeExecutor;
import com.flowagent.workflow.executor.VariableResolver;
import com.flowagent.workflow.memory.AgentMemory;
import com.flowagent.workflow.memory.AgentMemoryStore;
import com.flowagent.workflow.model.WorkflowNode;
import com.flowagent.workflow.model.WorkflowRun;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class AgentNodeExecutor implements NodeExecutor {
    private final ReActAgent reActAgent;
    // 查 agentId->角色定义/工具白名单
    private final AgentCatalog agentCatalog;
    private final CostTracker costTracker;
    private final AgentTracer agentTracer;
    private final WorkflowEngineProperties engineProperties;
    private final CostBudgetManager costBudgetManager;
    private final AgentMemoryStore memoryStore;

    public AgentNodeExecutor(ReActAgent reActAgent, AgentCatalog agentCatalog,
                             CostTracker costTracker, AgentTracer agentTracer,
                             WorkflowEngineProperties engineProperties,
                             CostBudgetManager costBudgetManager,
                             AgentMemoryStore memoryStore) {
        this.reActAgent = reActAgent;
        this.agentCatalog = agentCatalog;
        this.costTracker = costTracker;
        this.agentTracer = agentTracer;
        this.engineProperties = engineProperties;
        this.costBudgetManager = costBudgetManager;
        this.memoryStore = memoryStore;
    }

    @Override
    public Map<String, Object> execute(WorkflowNode node, Map<String, Object> input, WorkflowRun run) throws Execption {
        WorkflowNode.AgentConfig config = node.requireAgentConfig();
        // 0.目录里有没有这个 Agent 的静态定义？没有就退化（仍会告警），不阻断执行
        AgentDefinition agentDef=agentCatalog.get(config.getAgentId()).orElse(null);
        // 1.System Prompt 只来自 AgentCatalog，节点上的 promptTemplate 是任务提示（user message）
        String systemPrompt=buildSystemPrompt(config, agentDef);
        // 2.任务模板先做 {{...}} 插值；插完还是空就退回到 input 里的 query/prompt 兜底
        String userQuery = interpolateTemplate(config.getPromptTemplate(), input);
        if (userQuery == null || userQuery.isBlank()) {
            userQuery = (String) input.getOrDefault("query",
                    input.getOrDefault("prompt", "请根据上下文执行任务"));
        }
        // 3.执行参数三级优先级：节点 agentConfig > 运行级 input > 全局 yml 默认
        AgentExecutionSettings settings = AgentExecutionSettings.resolve(
                config, input, engineProperties != null ? engineProperties.getAgent() : null);
        int maxIterations = settings.maxIterations();
        HistoryPolicy historyPolicy = settings.historyPolicy();
    }
}
```
