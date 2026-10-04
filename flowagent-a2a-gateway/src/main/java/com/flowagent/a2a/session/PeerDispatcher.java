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
import java.util.concurrent.atomic.AtomicLong;

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

    /** 正在跑：认领成功、还没到终态。重放一律拦下，直到 TTL 到期。 */
    private static final String RUNNING = "RUNNING";

    /**
     * 跑完了，且该说的已经说出去了：回复已送达（无论 {@code success} 真假），或本来就
     * 无事可做/不必再试（消息不可读、kind 不该执行、广播没指定执行者、会话已收尾）。
     * <p>
     * 它才是「永久拒绝重放」的判据。<b>回复已送达的失败也算终态</b>——请求方已经拿到
     * 答复，重跑等于把一个已送达的答复作废，而且会投出第二条 {@code replyTo} 相同的
     * REPLY。javadoc 里「同伴明确告诉你它失败了」与「同伴没回话」是两种诊断信息，
     * 说的就是这件事。
     * </p>
     */
    private static final String SUCCEEDED = "SUCCEEDED";

    /**
     * 跑过了，但该说的话没能说出去（回投失败且不是会话已收尾）。允许重投抢回重跑。
     * <p>
     * <b>它只解除封锁，不发起重试。</b>系统里没有任何东西会自动重投——本地派发路径
     * 失败后没人再叫它，能重新到达的只有上游重投 {@code /wake}（运维手工重试、上游
     * 重试中间件）。所以这个状态的语义是「这条消息不再是死消息」，不是「它会被自动重跑」。
     * 进程内自动重试需要把尝试次数写进 guard 值、加退避与次数上限，是另一件事。
     * </p>
     */
    private static final String FAILED = "FAILED";

    private final SessionStore store;
    private final HttpClient httpClient;
    private final RedissonClient redissonClient;
    private final A2aSignature signature;

    /**
     * 幂等闸读不到（Redis 异常）因而放行、<b>没有做去重</b>的次数。
     * <p>
     * fail-open 是有意的（见 {@link #markHandled} 的说明），但「放行了」与「拦住了」
     * 在外部看起来一模一样 —— 这个计数是它们之间唯一的区分面。降级期间的重放会实打实
     * 再烧一次 token，没有这个数就看不见。
     * </p>
     */
    private final AtomicLong dedupeFailures = new AtomicLong();

    /**
     * 跑完但没送达、已被标成 {@link #FAILED} 的次数。
     * <p>
     * 与 {@link #dedupeFailures} 刻意分开：那个是「闸坏了所以没防住」，这个是
     * 「防住了但这一次没成功、需要重投」。合并成一个数就分不清该修闸还是该重投。
     * </p>
     */
    private final AtomicLong retryableFailures = new AtomicLong();

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
                boolean settled;
                try {
                    settled = handle(sessionId, messageId);
                } catch (Exception e) {
                    // 后台线程里的异常没人接，会把一次失败吞成静默。这里必须兜住并留痕。
                    // 「异常逃出来了」不是终态：该说的没说出去，允许日后重投。
                    log.error("[A2A] 处理消息失败 | session={} | message={} | error={}",
                            sessionId, messageId, e.toString(), e);
                    settled = false;
                }
                if (!settled) {
                    retryableFailures.incrementAndGet();
                }
                // 终态写入与 handle 的结论分开 try/catch，也不放进 finally：
                // handle 已经把每种失败都转成了值，终态写入再往上抛只会把原始结论盖掉。
                if (settled) {
                    markSettled(messageId, SUCCEEDED);
                } else {
                    markSettled(messageId, FAILED);
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
     * 幂等闸读不到（Redis 异常）因而放行、没做去重的累计次数。
     * <p>
     * 正常路径恒为 {@code 0}。它开始增长就说明「去重这道闸今天是关着的」，此时的重放
     * 会真的再跑一次 Agent；而这个数停在 {@code 0} 与「一条重复都没发生」在外部
     * 看起来完全一样 —— 这正是要把它单独露出来的原因。
     * </p>
     */
    public long dedupeFailures() {
        return dedupeFailures.get();
    }

    /**
     * 跑完但没送达、已被标成 {@code FAILED} 的累计次数。
     * <p>
     * 它不代表重试在发生（系统没有自动重试），而是「有多少条消息从死消息变回了可重投」。
     * 与 {@link #dedupeFailures()} 分属两个方向，别相加。
     * </p>
     */
    public long retryableFailures() {
        return retryableFailures.get();
    }

    /**
     * 抢占「这条消息由我接手」。
     * <p>
     * 三步，都是原子的、都必要：
     * </p>
     * <ol>
     *   <li>{@code trySet(RUNNING)} —— 没人认领过（或已过期）就是我的。</li>
     *   <li>{@code compareAndSet(FAILED, RUNNING)} —— <b>上一次跑完但没送达</b>，允许重投抢回。
     *       这一步本身就是「键不存在、或值不是 FAILED，就返回 false」，所以不需要先
     *       {@code get} 一次再判断：那会多一次往返，而且读到的值在判断时已经可能过期。</li>
     *   <li>再 {@code trySet(RUNNING)} 一次 —— 只接「键在第 1 步返回 false、到这一步之间
     *       恰好 TTL 到期」这一种。用 {@code trySet} 而不是 {@code get}：键真的消失时
     *       要的是「认领」，不是「读一个 null 出来再决定」。</li>
     * </ol>
     * <p>
     * 最多两次 {@code trySet} 加一次 {@code compareAndSet}，必然终止，不留循环。
     * </p>
     *
     * @return 抢到返回 {@code true}；已由别人接手（{@code RUNNING}）或已成功（{@code SUCCEEDED}）
     *         返回 {@code false}
     */
    private boolean markHandled(String messageId) {
        if (messageId == null) {
            // 没有 id 就没有去重的依据。放行而不是拦住：拦住等于把一条正常消息丢掉
            return true;
        }
        try {
            RBucket<String> guard = redissonClient.getBucket(
                    CacheConstants.A2A_SESSION_HANDLED + messageId);
            // 第一步：用 trySet 而不是 get + set —— 那两步之间是竞态窗口，
            // 两条并发重放会同时读到 null。值与 TTL 一次写入，不拆成 set + expire。
            if (guard.trySet(RUNNING, handledTtlSeconds, TimeUnit.SECONDS)) {
                return true;
            }
            // 第二步：上一次跑完但没送达的，允许重来
            if (guard.compareAndSet(FAILED, RUNNING)) {
                return true;
            }
            // 第三步：只接「键刚过期」这一种
            return guard.trySet(RUNNING, handledTtlSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Redis 不可用时不阻断执行：幂等是省钱的加固，不是正确性前提；反过来
            // 「拿不到闸门就不干活」会让一次 Redis 抖动变成一堆永远不执行的委派。
            // 但放行必须留痕：这条路径下的重放会实打实再烧一次 token，而它在外面
            // 看起来和「拦住了」一模一样 —— 计数是唯一的区分面。
            long n = dedupeFailures.incrementAndGet();
            if (n == 1 || n % 100 == 0) {
                // 计数逐次，日志限流（与 CostTracker#record 同一写法）
                log.warn("[A2A] 幂等闸读不到，本次放行不做去重 | 累计 {} 次 | message={} | error={}",
                        n, messageId, e.toString());
            }
            return true;
        }
    }

    /**
     * 写终态。
     * <p>
     * <b>用 {@code compareAndSet(RUNNING, ...)} 而不是 {@code set(...)}</b>。因为这次引入了
     * 「FAILED 可以被回收」这条路，原先「认领是排他的」这个隐含前提被削弱了：一次跑得特别久
     * 的执行（TTL 到期、重投已经抢回并跑完）回头再写终态时，无条件覆盖会把新一轮的
     * {@code RUNNING} 顶掉。CAS 以「我以为自己还在 RUNNING」为条件，迟到的旧执行者
     * 静默退场。
     * </p>
     * <p>
     * <b>不顺手刷新 TTL</b>：{@code compareAndSet} 不带 TTL，补一次 {@code expire} 就是第二条
     * 命令，两者之间会造出一个「状态已是终态、TTL 已到期」的窗口 —— 那恰好是要防的重复执行
     * 窗口。沿用认领时的 TTL，与「TTL 必须大于一次执行的最长耗时」是同一条不变量的两面。
     * </p>
     * <p>
     * Redis 异常只 WARN 不上抛：走到这里执行早就结束了，为了一个标记把结论变成异常不划算。
     * </p>
     */
    private void markSettled(String messageId, String state) {
        if (messageId == null) {
            return;
        }
        try {
            redissonClient.getBucket(CacheConstants.A2A_SESSION_HANDLED + messageId)
                    .compareAndSet(RUNNING, state);
        } catch (Exception e) {
            log.warn("[A2A] 幂等标记写入终态失败 | message={} | state={} | error={}",
                    messageId, state, e.toString());
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

    /**
     * 处理一条委派。
     * <p>
     * 返回的 {@code boolean} 是<b>终态（settled）</b>，不是「成没成功」：{@code true}
     * 表示这件事在本次处理里已经有了结论，重投不会得到不同的结果；{@code false}
     * 表示该说的没说出去，允许日后重投抢回。判据只有一条 —— <b>「再说一遍会不会
     * 产生不同的东西」</b>，与「结果好不好」无关。
     * </p>
     *
     * @return settled —— 见上
     */
    private boolean handle(String sessionId, String messageId) throws Exception {
        A2aMessage request = findMessage(sessionId, messageId);
        if (request == null) {
            // 会话过期后唤醒才到，或消息 id 对不上。都不是错误路径，不必惊动告警。
            // settled=true：消息都读不到，重投还是读不到，重试无意义
            log.debug("[A2A] 唤醒的消息已不可读（会话过期？） | session={} | message={}",
                    sessionId, messageId);
            return true;
        }

        if (!EXECUTABLE_KINDS.contains(request.getKind())) {
            // settled=true：本就不该执行，没有一回「跑」可以补
            log.debug("[A2A] 这不是一件要做的事，不执行 | session={} | message={} | kind={}",
                    sessionId, messageId, request.getKind());
            return true;
        }

        String target = request.getToAgentId();
        if (target == null || target.isBlank()) {
            // 广播的请求没有确定的执行者。不回话 —— 回给谁都不对。
            // settled=true：这不是「这次没能确定执行者」，是这条消息本身就无法确定，
            // 重投一次也还是同一个 null
            log.warn("[A2A] 广播的 {} 没有指定执行者，无法处理 | session={} | message={}",
                    request.getKind(), sessionId, messageId);
            return true;
        }

        Map<String, Object> payload = request.getPayload() == null ? Map.of() : request.getPayload();
        String task = payload.get("task") == null ? "" : String.valueOf(payload.get("task"));
        Object rawDepth = payload.get("depth");
        Map<String, Object> context = asMap(payload.get("context"));

        log.info("[A2A] 执行委派 | session={} | from={} | to={} | depth={}",
                sessionId, request.getFromAgentId(), target, rawDepth);

        return replyTo(sessionId, request, target, runAgent(target, task, rawDepth, context));
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
     * <p>
     * <b>返回的是 settled，而且「成功投出去」和「投不出去」都可能归到 {@code true}</b>：
     * 只要答复送出去了（无论里面写的 {@code success} 是真是假），对方就已经拿到了答案，
     * 这里就是终态。把「已送达」当成可重投，等于让一个已经生效的答复作废，还会投出
     * 第二条 {@code replyTo} 相同的 REPLY。
     * </p>
     *
     * @return settled —— 投递被明确拒绝（会话已收尾）或已送达为 {@code true}；
     *         因故障没送达为 {@code false}，允许重投
     */
    private boolean replyTo(String sessionId, A2aMessage request, String target,
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
            return true;
        } catch (FlowAgentException e) {
            if ("SESSION_CLOSED".equals(e.getErrorCode())) {
                // 发起方等超时后把会话收了。这不是故障 —— 会话里已经没人在听，
                // 把回复收下才是骗人。settled=true：会话不会活过来，重投一直撞同一面墙
                log.info("[A2A] 回复未送达（发起方可能已收尾会话） | session={} | replyTo={} | error={}",
                        sessionId, request.getMessageId(), e.getMessage());
                return true;
            }
            // 其余错误码是真实的投递故障（502 抖动、签名不符、网关重启……）。今天这条
            // 走的是 INFO，和上面那条「正常收尾」共用一行日志 —— 真正的故障被渲染成
            // 「已收尾」，从日志上根本看不出这是一条需要人来捞的消息。降到 WARN，
            // 并交出 settled=false，让标记落到 FAILED，上游重投还有机会
            log.warn("[A2A] 回复投递失败，可重投 | session={} | replyTo={} | code={} | error={}",
                    sessionId, request.getMessageId(), e.getErrorCode(), e.getMessage());
            return false;
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
