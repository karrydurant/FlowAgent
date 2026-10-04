```java
@Override
    public List<ToolDescriptor> listTools() {
        try {
            Map<String, Object> resp = get("/a2a/agents");
            Object data = resp == null ? null : resp.get("data");
            if (!(data instanceof List<?> agents)) {
                return List.of();
            }

            List<ToolDescriptor> tools = new ArrayList<>(agents.size());
            for (Object raw : agents) {
                if (!(raw instanceof Map<?, ?> agent)) {
                    continue;
                }
                Object agentId = agent.get("agentId");
                if (agentId == null || String.valueOf(agentId).isBlank()) {
                    continue;
                }
                tools.add(new ToolDescriptor(String.valueOf(agentId),
                        describe(agent), taskSchema()));
            }
            return tools;
        } catch (InterruptedException e) {
            // 见类注释的中断契约。listTools 每轮 buildPrompt 都会被调用，吞掉它等于让
            // 循环在引擎已放弃节点之后继续跑。
            Thread.currentThread().interrupt();
            throw new FlowAgentException("A2A_PEER_LIST_INTERRUPTED",
                    "列举可委派 Agent 时被中断（引擎节点超时/取消）", 500, e);
        } catch (Exception e) {
            log.warn("[PeerBridge] 列举可委派 Agent 失败，按「无同伴」处理 | base={} | error={}",
                    a2aGatewayUrl, messageOf(e));
            return List.of();
        }
    }
```
