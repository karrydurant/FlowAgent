# checkpoint

就是把整个 `WorkflowRun` 对象拍扁成 JSON

```json
{
  "runId": "wfrun-a1b2c3d4e5f6",
  "workflowId": "bug-fix-workflow",
  "workflowVersion": "1.0.0",
  "status": "RUNNING",

  "activeNodeIds": ["node-3"],
  "completedNodeIds": ["node-1", "node-2"],
  "failedNodeIds": [],
  "skippedNodeIds": [],
  "blockedNodeIds": [],

  "nodeResults": {
    "node-1": {
      "nodeId": "node-1",
      "nodeName": "代码审查Agent",
      "status": "COMPLETED",
      "output": {"reviewResult": "发现3个问题"},
      "startedAt": "2026-10-10T10:00:00Z",
      "completedAt": "2026-10-10T10:00:15Z",
      "durationMs": 15234,
      "retryCount": 0
    },
    "node-2": {
      "nodeId": "node-2",
      "nodeName": "严重程度判断",
      "status": "COMPLETED",
      "output": {"severity": "HIGH"},
      "startedAt": "2026-10-10T10:00:16Z",
      "completedAt": "2026-10-10T10:00:22Z",
      "durationMs": 6123,
      "retryCount": 0
    }
  },

  "variables": {
    "bugDescription": "登录接口500",
    "node:node-1:output": {"reviewResult": "发现3个问题"},
    "node:node-2:output": {"severity": "HIGH"},
    "condition:node-2": "high"
  },

  "input": {
    "bugId": "BUG-12345"
  },

  "timeline": [
    {"timestamp": "2026-10-10T10:00:00Z", "eventType": "WORKFLOW_SUBMITTED", "nodeId": null, "description": "工作流已提交"},
    {"timestamp": "2026-10-10T10:00:00Z", "eventType": "NODE_START", "nodeId": "node-1", "description": "开始执行: 代码审查Agent"},
    {"timestamp": "2026-10-10T10:00:15Z", "eventType": "NODE_COMPLETE", "nodeId": "node-1", "description": "执行完成 (15234ms)"},
    {"timestamp": "2026-10-10T10:00:16Z", "eventType": "NODE_START", "nodeId": "node-2", "description": "开始执行: 严重程度判断"}
  ],

  "retryCount": 0,
  "resumeCount": 0,
  "createdAt": "2026-10-10T10:00:00Z",
  "startedAt": "2026-10-10T10:00:00Z",

  "lastCheckpointNodeId": "node-2",
  "definitionFingerprint": "a1b2c3d4e5f6a7b8",

  "suspendedApprovalNodeId": null,
  "suspendedApprovalTaskId": null
}
```
