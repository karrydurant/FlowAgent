package com.flowagent.a2a.session;

import com.flowagent.common.constant.CacheConstants;
import com.flowagent.common.exception.FlowAgentException;
import com.flowagent.common.security.A2aSignature;
import com.flowagent.common.util.JsonUtil;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 「有人给你发了活」之后，真正去把活干完的那一环。
 *
 * <h3>它在整条链上的位置</h3>
 * <pre>
 *   发起方 A（可能是另一个进程）
 *        │  POST /a2a/sessions/{id}/messages   ← 消息落进共享 Redis
 *        ▼
 *   SessionManager.deliver
 *        ├── 接收方在本实例 ──→ 本类.dispatch()            （直接调，不走网络）
 *        └── 接收方在别的实例 ─→ POST {对端}/sessions/{id}/wake
 *                                    │
 *                                    ▼
 *                           对端 A2aController.wake ──→ 对端 本类.dispatch()
 *        ▼
 *   本类：读消息 → 跑目标 Agent → POST REPLY 回会话
 * </pre>
 * <p>
 * 两条入口汇到同一个方法，所以「本地委派」与「跨进程委派」在行为上没有分叉 ——
 * 分叉过的投递路径正是这次要消灭的东西。
 * </p>
 *
 * <h3>为什么不注入 SessionManager，而是走 HTTP 回投</h3>
 * <p>
 * 本类要把结果作为一条 {@code REPLY} 写回会话，最直接的做法是注入
 * {@code SessionManager} 调 {@code post}。但那会造出一个循环依赖：
 * {@code SessionManager.deliver} 要调本类，本类又要调 {@code SessionManager}。
 * </p>
 * <p>
 * 所以回投走 HTTP（打本实例的 {@code POST /a2a/sessions/{id}/messages}）。
 * 这条路顺带把「消息只能由会话 API 写进会话」变成结构上无法绕过的事实：
 * 本类读会话用存储、写会话用 API，没有第二条写路径可以悄悄绕开校验与预算。
 * 同一模块里 {@code DelegationManager} 用 HTTP 打本机 {@code /agents/{id}/run}，
 * 是同一取舍的先例。
 * </p>
 *
 * <h3>只有「要别人做事」的 kind 会被执行</h3>
 * <p>
 * 可执行的是 {@code REQUEST}（委派）与 {@code SOLICIT}（征询意见）—— 这两个都是
 * 「对方得跑一遍自己的 Agent 才能回答」。其余 kind（{@code REPLY}/{@code PROPOSE}/
 * {@code VOTE}/{@code INFORM}）只落盘、不执行。
 * </p>
 * <p>
 * 这既是语义上的正确（回复不是新任务），也是一条防环的硬约束：本类执行完会回投，
 * 而回投的 kind 不在可执行集里，链条到此为止 —— 若不按 kind 拦一道，
 * A→B→A→B 就会自己转起来。{@code PROPOSE} 尤其不能放进来：成员随手表个态
 * 就会把对方叫起来干一遍活。
 * </p>
 *
 * <h3>答复的 kind 由请求的 kind 决定</h3>
 * <p>
 * {@code REQUEST} → {@code REPLY}（做完的事），{@code SOLICIT} → {@code PROPOSE}
 * （一个立场）。这条规则放在这里而不是让调用方指定：调用方指定就成了
 * 「消息自己声明自己是什么」，收不到任何校验；而按请求推导，日志里
 * 「一问一答」永远配对得上。
 * </p>
 */
@Slf4j
@Component
public class PeerDispatcher {

    /**
     * 会触发执行的 kind —— 只有「对方得跑一遍自己的 Agent 才能回答」的那两种。
     * <p>
     * 见类注释「只有『要别人做事』的 kind 会被执行」。写成常量集合而不是就地写两个
     * {@code if}：这条例线是防环的承重墙，它应当只有一处定义、且能被测试整体钉住。
     * </p>
     */
    private static final Set<A2aMessage.MessageKind> EXECUTABLE_KINDS =
            EnumSet.of(A2aMessage.MessageKind.REQUEST, A2aMessage.MessageKind.SOLICIT);

    private final SessionStore store;
    private final HttpClient httpClient;
    private final RedissonClient redissonClient;
    private final A2aSignature signature;

    /**
     * 本实例的基址 —— 目标 Agent 的执行端点与消息回投端点都挂在它下面。
     * <p>
     * 与 {@code SessionManager} 用同一个配置项：都是「我在哪」这一个事实。
     * </p>
     */
    @Value("${flowagent.a2a.local-base-url:${flowagent.gateways.a2a.base-url:http://localhost:8080}}")
    private String localBaseUrl;

    /** 跑一个目标 Agent 的超时（秒）。超时即放弃，把失败作为 REPLY 投回去。 */
    @Value("${flowagent.a2a.dispatch-timeout-seconds:60}")
    private long dispatchTimeoutSeconds = 60L;

    /**
     * 「这条消息已接手」标记的存活时间（秒）。
     * <p>
     * 必须 <b>大于</b> 一次执行的最长耗时（{@code dispatch-timeout-seconds}），否则
     * 标记会在执行还没结束时就过期，重放的唤醒又能挤进来跑第二遍 —— 这道闸门就白设了。
     * 默认值是它的 10 倍，留足余量。
     * </p>
     */
    @Value("${flowagent.a2a.handled-ttl-seconds:600}")
    private long handledTtlSeconds = 600L;

    /**
     * 执行同伴任务的线程池。
     * <p>
     * <b>与协商的池分开</b>：协商是「一轮里并发问 N 个 Agent 然后全部等回来」，
     * 本池是「后台把一件委派做完」。合成一个池，一次 N 方协商就会把委派任务挤到队尾。
     * </p>
     * <p>
     * 拒绝策略是 <b>Abort</b>，不是 {@code CallerRunsPolicy}：队列打满说明并发委派
     * 已经超载，此时让调用线程（发起方的 {@code POST /messages}，或者跨实例那条
     * 唤醒请求的线程）去跑一整个 Agent，等于把发起方按在这里几十秒。宁可
     * 明确报 503，让调用方看见「现在接不下」并自己决定重试。
     * </p>
     */
    private final ThreadPoolExecutor pool;

    public PeerDispatcher(SessionStore store, HttpClient httpClient,
                          RedissonClient redissonClient, A2aSignature signature) {
        this.store = store;
        this.httpClient = httpClient;
        this.redissonClient = redissonClient;
        this.signature = signature;

        AtomicInteger seq = new AtomicInteger();
        this.pool = new ThreadPoolExecutor(
                2, 8, 60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(200),
                r -> {
                    Thread t = new Thread(r, "a2a-peer-" + seq.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * 把一条消息交给后台处理。
     * <p>
     * 立即返回，不等执行完成 —— 投递方（尤其是跨实例那条唤醒请求）不应该被对方的
     * 推理耗时按在原地。
     * </p>
     *
     * @throws FlowAgentException {@code DELIVER_FAILED}(503) 线程池已满
     */
    public void dispatch(String sessionId, String messageId) {
        // 幂等闸：同一条消息只接手一次。位置是承重的 —— 必须在提交线程池**之前**、
        // 且在调用方的线程上同步做，否则两个并发的重放可能各自都通过了检查才开始跑。
        //
        // 键用 messageId 而不是 (sessionId, messageId)：只有 REQUEST 会被执行，而
        // REQUEST 必然带 toAgentId（见 handle），所以它在本实例上的接收方恰好只有一个，
        // 不会出现「一条广播消息要交给多个目标、却被这道闸拦掉后面的」。
        if (!markHandled(messageId)) {
            log.info("[A2A] 这条消息已接手过，跳过重复派发 | session={} | message={}",
                    sessionId, messageId);
            return;
        }

        try {
            pool.execute(() -> {
                try {
                    handle(sessionId, messageId);
                } catch (Exception e) {
                    // 后台线程里的异常没人接，会把一次失败吞成静默。这里必须兜住并留痕。
                    log.error("[A2A] 处理消息失败 | session={} | message={} | error={}",
                            sessionId, messageId, e.toString(), e);
                }
            });
        } catch (RejectedExecutionException e) {
            // 拒了就当没接手过：否则这条消息被永久标记为"已处理"，重投也会被闸门拦掉 ——
            // 一次过载变成一个再也不会被执行的死消息。
            releaseHandled(messageId);
            throw FlowAgentException.of("DELIVER_FAILED",
                    "本实例的委派执行队列已满，暂时接不下新的委派（session="
                            + sessionId + "）。消息已落盘，稍后可重新投递。", 503);
        }
    }

    /**
     * 抢占「这条消息由我接手」。
     *
     * @return 抢到返回 {@code true}；已被抢过返回 {@code false}
     */
    private boolean markHandled(String messageId) {
        if (messageId == null) {
            // 没有 id 就没有去重的依据。放行而不是拦住：拦住等于把一条正常消息丢掉
            return true;
        }
        try {
            RBucket<String> guard = redissonClient.getBucket(
                    CacheConstants.A2A_SESSION_HANDLED + messageId);
            // 用 trySet 而不是 get + set：那两步之间是竞态窗口，两条并发重放会同时读到 null。
            // 值与 TTL 一次写入，不拆成 set + expire —— 漏写第二步就是永久 key。
            return guard.trySet("1", handledTtlSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Redis 不可用时不阻断执行：幂等是省钱的加固，不是正确性前提；反过来
            // 「拿不到闸门就不干活」会让一次 Redis 抖动变成一堆永远不执行的委派。
            log.warn("[A2A] 幂等标记写入失败，本次不做去重 | message={} | error={}",
                    messageId, e.toString());
            return true;
        }
    }

    private void releaseHandled(String messageId) {
        if (messageId == null) {
            return;
        }
        try {
            redissonClient.getBucket(CacheConstants.A2A_SESSION_HANDLED + messageId).delete();
        } catch (Exception e) {
            log.warn("[A2A] 幂等标记释放失败 | message={} | error={}", messageId, e.toString());
        }
    }

    // ======================== 处理 ========================

    private void handle(String sessionId, String messageId) throws Exception {
        A2aMessage request = findMessage(sessionId, messageId);
        if (request == null) {
            // 会话过期后唤醒才到，或消息 id 对不上。都不是错误路径，不必惊动告警
            log.debug("[A2A] 唤醒的消息已不可读（会话过期？） | session={} | message={}",
                    sessionId, messageId);
            return;
        }

        if (!EXECUTABLE_KINDS.contains(request.getKind())) {
            log.debug("[A2A] 这不是一件要做的事，不执行 | session={} | message={} | kind={}",
                    sessionId, messageId, request.getKind());
            return;
        }

        String target = request.getToAgentId();
        if (target == null || target.isBlank()) {
            // 广播的请求没有确定的执行者。不回话 —— 回给谁都不对
            log.warn("[A2A] 广播的 {} 没有指定执行者，无法处理 | session={} | message={}",
                    request.getKind(), sessionId, messageId);
            return;
        }

        Map<String, Object> payload = request.getPayload() == null ? Map.of() : request.getPayload();
        String task = payload.get("task") == null ? "" : String.valueOf(payload.get("task"));
        Object rawDepth = payload.get("depth");
        Map<String, Object> context = asMap(payload.get("context"));

        log.info("[A2A] 执行委派 | session={} | from={} | to={} | depth={}",
                sessionId, request.getFromAgentId(), target, rawDepth);

        replyTo(sessionId, request, target, runAgent(target, task, rawDepth, context));
    }

    /**
     * 跑目标 Agent。
     * <p>
     * 打的是本进程的 {@code POST /agents/{agentId}/run} —— 复用既有的试运行端点，
     * 它读该 Agent 自己的 systemPrompt 与 allowedTools，跑完整 ReAct 循环。
     * 刻意不为委派另造一个执行端点：那会变成两套「跑一个 Agent」的实现。
     * </p>
     * <p>
     * <b>任何失败都变成一张带 {@code success=false} 的结果</b>，而不是抛出去。
     * 抛出去等于对方永远等不到 REPLY、只能等到超时 —— 把「同伴明确告诉你它失败了」
     * 降级成「同伴没回话」，那是两种完全不同的诊断信息。
     * </p>
     */
    private Map<String, Object> runAgent(String target, String task, Object depth,
                                         Map<String, Object> context) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("query", task);

        Map<String, Object> runContext = new LinkedHashMap<>();
        if (context != null && !context.isEmpty()) {
            // 委派的 context 是<b>调用方给的业务上下文</b>（{@code /a2a/delegate} 的
            // {@code task.context}），必须原样带给目标 Agent。
            runContext.putAll(context);
        }
        if (depth != null) {
            // 深度随调用链往下传：目标 Agent 若再委派，它的 PeerAgentToolBridge 从这个
            // context 里读到当前层数，才能把链条截断在 max-delegation-depth。
            // 放在最后一行：调用方在 context 里塞一个同名的键也覆盖不掉链条位置，
            // 那是本实例知道的事实，不是它可以声明的东西。
            runContext.put("a2a.delegationDepth", depth);
        }
        if (!runContext.isEmpty()) {
            body.put("context", runContext);
        }

        try {
            // scope 传 null：/agents/{id}/run 是本进程内部端点，不属于 A2A 会话通道，
            // 不参与签名。签名只覆盖「一条消息进入某个会话」这件事。
            Map<String, Object> envelope = post("/agents/" + target + "/run", body,
                    dispatchTimeoutSeconds, null);
            Object data = envelope == null ? null : envelope.get("data");
            if (!(data instanceof Map<?, ?> result)) {
                return failure(target, "Agent 执行端点返回空结果");
            }

            Map<String, Object> reply = new LinkedHashMap<>();
            reply.put("agentId", target);
            reply.put("success", Boolean.TRUE.equals(result.get("success")));
            reply.put("answer", result.get("finalAnswer"));
            reply.put("totalSteps", result.get("totalSteps"));
            reply.put("error", result.get("errorMessage"));
            return reply;
        } catch (InterruptedException e) {
            // 后台线程同样要守中断契约：还原中断位，再如实上报
            Thread.currentThread().interrupt();
            return failure(target, "执行被中断");
        } catch (Exception e) {
            log.warn("[A2A] 执行 Agent 失败 | agent={} | error={}", target, e.toString());
            return failure(target, e.getMessage() != null ? e.getMessage() : e.toString());
        }
    }

    private static Map<String, Object> failure(String target, String error) {
        Map<String, Object> reply = new LinkedHashMap<>();
        reply.put("agentId", target);
        reply.put("success", false);
        reply.put("error", error);
        return reply;
    }

    /**
     * 把执行结果投回会话。
     * <p>
     * 答复的 kind 跟着请求走：{@code SOLICIT} 问的是立场，答回去的是一条
     * {@code PROPOSE}；其余（{@code REQUEST}）答回去的是 {@code REPLY}。
     * </p>
     */
    private void replyTo(String sessionId, A2aMessage request, String target,
                         Map<String, Object> result) throws Exception {
        A2aMessage.MessageKind replyKind =
                request.getKind() == A2aMessage.MessageKind.SOLICIT
                        ? A2aMessage.MessageKind.PROPOSE
                        : A2aMessage.MessageKind.REPLY;

        A2aMessage reply = A2aMessage.builder()
                .fromAgentId(target)
                .toAgentId(request.getFromAgentId())
                .kind(replyKind)
                .replyTo(request.getMessageId())
                .payload(result)
                .build();

        try {
            post("/a2a/sessions/" + sessionId + "/messages", toBody(reply),
                    dispatchTimeoutSeconds, sessionId);
            log.info("[A2A] 执行完成并已回投 | session={} | to={} | replyTo={} | kind={} | success={}",
                    sessionId, request.getFromAgentId(), request.getMessageId(), replyKind,
                    result.get("success"));
        } catch (FlowAgentException e) {
            // 最常见的一种：发起方等超时后把会话收了（SESSION_CLOSED）。这不是故障 ——
            // 会话里已经没人在听，把回复收下才是骗人。记一条 INFO 就够了。
            log.info("[A2A] 回复未送达（发起方可能已收尾会话） | session={} | replyTo={} | error={}",
                    sessionId, request.getMessageId(), e.getMessage());
        }
    }

    /** {@link A2aMessage} → 投递端点的请求体。{@code null} 值一律不发，让服务端补默认。 */
    private static Map<String, Object> toBody(A2aMessage message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("fromAgentId", message.getFromAgentId());
        body.put("toAgentId", message.getToAgentId());
        body.put("kind", message.getKind().name());
        body.put("replyTo", message.getReplyTo());
        body.put("payload", message.getPayload());
        if (message.getImportance() != null) {
            body.put("importance", message.getImportance().name());
        }
        return body;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object raw) {
        return raw instanceof Map ? (Map<String, Object>) raw : null;
    }

    private A2aMessage findMessage(String sessionId, String messageId) {
        if (sessionId == null || messageId == null) {
            return null;
        }
        for (A2aMessage message : store.readAfter(sessionId, 0)) {
            if (messageId.equals(message.getMessageId())) {
                return message;
            }
        }
        return null;
    }

    // ======================== HTTP ========================

    @SuppressWarnings("unchecked")
    private Map<String, Object> post(String path, Map<String, Object> body, long timeoutSeconds,
                                     String scope) throws Exception {
        // 序列化一次：待签串与真正发出去的报文必须是同一个串，见 SessionManager#wake 同一处说明
        String payload = JsonUtil.toJson(body);

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(localBaseUrl + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(timeoutSeconds));

        // scope 为 null 的端点不签名（/agents/{id}/run）；headersFor 对 null 返回空表
        signature.headersFor(scope, payload).forEach(builder::header);

        HttpResponse<String> response = httpClient.send(builder.build(),
                HttpResponse.BodyHandlers.ofString());

        Map<String, Object> envelope = null;
        try {
            envelope = JsonUtil.fromJson(response.body(), Map.class);
        } catch (Exception ignored) {
            // 非 JSON 响应（HTML 错误页等）：下面按状态码报错，信封保持 null
        }

        if (response.statusCode() / 100 != 2) {
            String code = envelope == null ? null : String.valueOf(envelope.get("errorCode"));
            String message = envelope == null ? null : String.valueOf(envelope.get("errorMessage"));
            throw FlowAgentException.of(
                    code != null && !"null".equals(code) ? code : "A2A_DISPATCH_FAILED",
                    "调用 " + path + " 返回 HTTP " + response.statusCode()
                            + (message != null && !"null".equals(message) ? "：" + message : ""),
                    502);
        }
        return envelope;
    }

    @PreDestroy
    public void shutdown() {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

