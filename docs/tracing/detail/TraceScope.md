# TraceScope

```java
package com.flowagent.common.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 一个手动管理的 span 作用域
 *
 * 与 AgentTracer 的关系：AgentTracer 负责 "工作流/节点" 这类带业务计数器的 span，本类负责所有
 * 没有 额外业务状态 的 span（MCP 工具调用、出站请求、试跑根 span）。两者都落在同一个 Tracer 上，所以父子关系
 * traceId、导出路径完全一致。
 *
 * 两种用法：
 * 1.start(String, Map)：从现在开始计时，并在当前线程打开作用域 -- 作用域内创建的 span 都是它的子 span。用
 * try-with-resources 关闭
 * 2.record(String, long, long, Map, Throwable)：调用 
 */
```
