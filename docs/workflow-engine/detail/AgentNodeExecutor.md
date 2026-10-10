# AgentNodeExecutor

```java
package com.flowagent.workflow.executor.impl;

import com.flowagent.common.exception.FlowAgentException;
import com.flowagent.workflow.agent.ReActAgent;
import com.flowagent.workflow.agent.ReActAgent.ReActResult;
import com.flowagent.workflow.executor.NodeExecutor;
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
    public AgentNodeExecutor(ReActAgent reActAgent) {
        this.reActAgent=reActAgent;
    }
    @Override
    public Map<String, Object> execute(WorkflowNode node, Map<String, Object> input, WorkflowRun run) throws Exception {
        WorkflowNode.AgentConfig config = node.requireAgentConfig();

        String systemPrompt = buildSystemPrompt(config, input);

        String userQuery=
    }
}
```
