# Tracing

## TraceScope

手动管理的 span 作用域，AutoCloseable。两种用法：start() 打开当前线程作用域、record() 补记
已结束调用。没装 tracer 时返回 NOOP 空实现。

## 
