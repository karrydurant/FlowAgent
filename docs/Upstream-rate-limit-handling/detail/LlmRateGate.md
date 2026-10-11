# LlmRateGate

## 定位

一个进程内的令牌桶，控制 LLM API 的发起速率，在每次发请求前先取一个令牌，没令牌就排队

## 什么是令牌桶

想象一个桶，里面装着令牌：

每发一个请求，从桶里拿一个令牌。没令牌就不能发，得等。

桶按固定速率自动补令牌。比如每秒补 0.3 个，就是每 3.33 秒补一个。

桶有容量上限（burst）。补到满就不补了 —— 防止攒一堆令牌然后瞬间全放出去，等于没限速。

## 按时间补令牌 -- refill()

```java
private void refill() {
    long now = nanoClock.getAsLong();
    long elapsedNanos = now - lastRefillNanos;
    if (elapsedNanos <= 0) return;
    tokens = Math.min(burst, tokens + elapsedNanos / 1_000_000_000.0 * permitsPerSecond);
    lastRefillNanos = now;
}
```

不是定时线程去补，而是每次有人来取令牌时，先算一下 "距上次过了多久，该补多少"。
懒加载 —— 没有请求就不补，省一个后台线程。

## 取令牌 -- acquire()

```java
public void acquire(String callSite) throws InterruptedException {
    if (!enabled) return;
    long startedNanos=nanoClock.getAsLong();
    while (true) {
        long needNanos;
        synchronized (this) {
            refill();                              // 先补
            if (tokens >= 1.0) {                   // 有令牌
                tokens -= 1.0;                     // 取走一个
                return;                            // 立刻返回，请求发出去
            }
            // 没令牌：算一下补满一个还要多久
            needNanos = (long) Math.ceil((1.0 - tokens) / permitsPerSecond * 1_000_000_000L);
        }

        //等太久就报错
        long waitedNanos = nanoClock.getAsLong() - startedNanos;
        if (maxWait != null && Duration.ofNanos(waitedNanos + needNanos).compareTo(maxWait) > 0) {
            throw new FlowAgentException(TIMEOUT_ERROR_CODE, "...", 503);
        }
        // 睡到下一个令牌补出来。Thread.sleep 可中断——被中断直接抛 InterruptedException
        Thread.sleep(Math.max(1, needNanos / 1_000_000));
    }
}
```
