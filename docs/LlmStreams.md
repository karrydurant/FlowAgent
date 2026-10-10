# LlmStreams

## Flux<ChatResponse>：

OpenAI 兼容的流式响应（SSE）-- 上游不等整段回答生成完，每吐几个 token 就发一个 HTTP chunk。
Spring AI 把每个 chunk 包成一个 ChatResponse，
一串就成了 Flux<ChatResponse>。.collectList() 就是 "等它推完，把所有帧攒成List给我"。

## 为什么非要流式：

非流式调用 call() 的语义是 "请求发出后阻塞到完整响应返回" 。对模型来说一轮完整的输出常达 30~80 秒，而底层
HTTP 客户端在 10 秒没有新字节时直接判定读超时

流式把超时语义从 "整段生成总时长" 换成 "两帧之间的间隔"：只要服务端还在持续推字节，读超时计时器就不断重置。慢但正常的长回答不会被误杀，
同时首字延迟从 "50 秒后" 降到 "1~2 秒后"，对交互式 Agent 体验是决定性的。
代价是响应被切碎：下游拿到的是 500 个独立帧，正文要自己拼、usage 要自己聚合、
`finishReason` / `model` 要从最后一帧提取 —— 这正是 `LlmStreams.aggregate` 存在的原因。

## Spring AI 的 advisor 链是一次性的：

chatClient.prompt(prompt).stream()，这一行并不会立即发 HTTP 请求，而是构造一个 `DefaultStreamResponseSpec`，
内部持有 `DefaultAroundAdvisorChain` -- 本质是一个 `Deque<StreamAdvisor>`（记日志，塞 conversationId，安全护栏等排成队列）
每次下游订阅这个流时，链通过 `nextStream` 逐个 pop() 取出 advisor，把请求层层包裹，最后一个 advisor 才真正发起 HTTP 调用

问题在于这个 `Deque` 是一次性的：`pop()` 一次少一个，订阅完成后队列已空。
如果下游把同一个 `Flux<ChatResponse>` 二次订阅（例如重试时直接对同一条流再 `.block()` 一次），
所有 advisor 都已出队，链抛 `IllegalStateException("No StreamAdvisors available to execute")` -- 第二次
连 HTTP 请求都不会发。

**这会造成什么问题**

假设你想给 LLM 调用加重试，直觉写法是

```java
Flux<ChatResponse> stream=chatClient.prompt(prompt).stream().chatResponse();

//第一次尝试
ChatResponse r1=stream.timeout(...).collectList().block();

//如果失败了
ChatResponse r2=stream.timeout(...).collectList().block();
```

预期是第一次收到 429，等 10 秒，第二次重新发送请求，成功

实际上：

第一次 block() 正常 -- advisor 链逐个 pop，发 HTTP 请求，收到 429

你捕获到 429，等 10 秒，第二次 `block()`——直接抛 `IllegalStateException: No StreamAdvisors available to execute`。

因为 advisor 链内部是个 Deque，每处理一个 advisor 就 pop() 一个。第一次订阅完，队列已经空，
第二次订阅时没有 advisor 可用，Spring AI 直接抛错，连 HTTP 请求都没有发出去

这个错误造成的后果：

1.重试根本没有生效

2.原始错误被盖掉了

3.白等退避时间

**代码里怎么修的**

`public static ChatResponse await(Supplier<Flux<ChatResponse>> stream, ...)`

调用方传进来的是 

`() -> chatClient.prompt(prompt).stream().chatResponse()`

每次重试时，重新调一次这个 lambda -- 也就是重新执行 prompt().stream()，重新建一条 advisor 链，重新发 
HTTP 请求。旧的流直接扔掉，根本不重订阅

```java
Mono<ChatResponse> attempt=Mono.defer(() -> {
    return guard(stream.get(), idleTimeout, ...)
            .collectList()
            .map(frames -> aggregate(frames, callSite));
});
```

Mono.defer 在这里的作用是：里面的 lambda 不是现在执行，而是每次有人订阅时才执行。
重试=重新订阅=重新执行lambda=重新造一条流
