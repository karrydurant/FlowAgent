# LlmCallTracingObserver

```java
package com.flowagent.observe.tracing;

import com.flowagent.common.llm.LlmCallObserver;
import com.flowagent.common.llm.LlmStreams;
import com.flowagent.common.metrics.FlowAgentMetrics;
import com.flowagent.common.tracing.TraceIds;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.metadata.ChatResponseMetadata;
import org.springframework.ai.chat.metadata.Usage;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class LlmCallTracingObserver implements LlmCallObserevr {
    private 
}
```
