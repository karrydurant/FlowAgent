#### WorkflowDefinition几个重要字段

~~id（String，必填）---- 唯一标识~~
~~name（String，必填）~~
~~nodes（List<WorkflowNode>）---- 节点表~~
~~edges（List<WorkflowEdge>）---- 边表~~

#### WorkflowRun几个重要字段

~~runId（String）~~
~~workflowId（String）~~
~~activeNodeIds（List<String>）~~
~~completedNodeIds（List<String>）~~
~~failedNodeIds（List<String>，CopyOnWriteArrayList）~~
~~completedNodeIds（List<String>，CopyOnWriteArrayList）
