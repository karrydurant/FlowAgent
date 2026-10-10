# double-run-guard

## runningWorkflows

`private final ConcurrentHashMap<String, WorkflowRun> runningWorkflows=new ConcurrentHashMap<>();

**所有用到它的地方**

| 位置 | 代码 | 干什么 |
| ---- | ---- | ---- |
| createRun() | runningWorkflows.put(runId, run) | 新工作流提交时，登记进去 |
| prepareResume() | runningWorkflows.containsKey(runId) | 并发校验：本 JVM 内有没有在跑 |
| prepareResume() | runningWorkflows.putIfAbsent(runId, run) | 原子占用 |
| executeInternal() finally | runningWorkflows.remove(runId) | 跑完了，移出去 |
| rejectRun() | runningWorkflows.remove(runId, run) | 抢租约失败了，把自己占的位置删掉 |
| getRun() | runningWorkflows.get(runId) | 查询时先查内存 |
| listRunning() | new ArrayList<>(runningWorkflows.values()) | 列出本副本正在跑的 |

## leaseManager 

WorkflowRunLeaseManager 就是一个 Redis 分布式锁的封装 -- 抢锁、查锁、放锁，外加看门狗自动续期和运维强删 

```java
package com.flowagent.workflow.lease;

import com.flowagent.common.constant.CacheConstants;
import com.flowagent.common.exception.FlowAgentException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class WorkflowRunLeaseManager {
    //错误码--抢锁失败的两种原因
    public static final String ERR_LEASE_NOT_ACQUIRED = "LEASE_NOT_ACQUIRED";
    public static final String ERR_LEASE_UNAVAILABLE = "LEASE_UNAVAILABLE";
    //事件类型
    public static final String EVENT_LEASE_REJECTED = "WORKFLOW_LEASE_REJECTED";
    public static final String EVENT_LEASE_ERROR = "WORKFLOW_LEASE_ERROR";
    //核心依赖 -- Redisson 客户端，用来操作 Redis 分布式锁
    private final RedissonClient redissonClient;
    @Value("${flowagent.workflow.lease.enabled:true}")
    //关掉等于回到 "只有本地去重、没有跨副本互斥"
    private boolean enabled = true;
    public String keyOf(String runId) {
        return CacheConstants.LOCK_WORKFLOW_RUN + runId;
    }
    public boolean isHeld(String runId) {
        if (!enabled) return false;
        return redissonClient.getLock(keyOf(runId)).isLocked();
    }
    public Optional<WorkflowRunLease> tryAcquire(String runId) {
        if (!enabled) {
            
        }
        RLock lock=redissonClient.getLock(keyOf(runId));
        boolean acquired=lock.tryLock(0, TimeUnit.SECONDS);
        return acquired ? Optional.of(new RedissonWorkflowRunLease(lock, runId)) : Optional.empty();
    }
    public void release(WorkflowRunLease lease, String runId) {
        if (lease==null) return;
        try {
            lease.close();
        } catch (Exception e) {
            l
        }
    }
    public long watchingMillis() {
        return redissonClient.getConfig().getLockWatchdogTimeout();
    }
    public boolean forceRelease(String runId) {
        return redissonClient.getLock(keyOf(runId)).forceUnlock();
    }

}
```
