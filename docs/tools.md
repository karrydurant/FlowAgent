## 智能体引入工具的方式

MCP 提供两套标准传输层(Transport)：stdio(本地)、HTTP+SSE(远程网络)

#### 本地进程类

1.Stdio（标准输入输出）JSON-RPC

MCP 本地 Server 主流方案，工具作为独立子进程运行，智能体通过 `stdin/stdout` 管道收发 JSON-RPC 消息，无网络开销，适合本地文件访问、沙盒代码执行、本地数据库查询等场景。依靠启动子进程，通过操作系统管道完成进程间通信。

2.Subprocess CLI

直接调用命令行程序，参数通过命令行传参，结果捕获 stdout/stderr。比如调用git、curl、本地脚本，框架里常封装成工具，缺点是参数逃逸风险、序列化麻烦。

3.内存函数直接注册（进程内函数）

框架原生`@tool`装饰器（LangChain / LlamaIndex），直接把同进程内 Python/JS 函数注册为工具。模型输出参数后，直接本地调用函数，不走网络。适合轻量计算、本地工具，最简单。

#### 网络传输协议

1.HTTP+SSE

基于 HTTP 构建的单向长连接方案，是 MCP 标准远程 Transport。客户端通过 GET 建立 SSE 长连接接收服务端推送消息，客户端下发指令使用独立 HTTP POST 接口；仅服务端单向推送。适合远程 MCP 服务、流式返回工具结果场景。与 WebSocket 区别：SSE 单向推送，WebSocket 支持双向消息交互。

2.WebSocket

双向长连接协议，支持客户端与服务端互发消息。适合工具需要持续推送流式结果、双向实时交互场景（浏览器自动化，实时日志）。MCP 也支持 WebSocket Transport。

3.JSON-RPC over HTTP

纯 POST JSON-RPC，和普通 REST 不同，固定 method+params 结构；MCP 底层就是 JSON-RPC 2.0

4.gRPC

protobuf 二进制协议，高性能内部服务。适合企业内部高吞吐工具服务，需要手动写适配器转成智能体可识别的input_schema。

5.消息队列

异步工具调用：智能体发消息丢队列，worker 消费执行，结果回写队列。适合长耗时任务（大文件处理、训练任务），异步非阻塞。

#### 通过规范文件批量声明工具

1.OpenAPI/Swagger

直接传入 OpenAPI 3.x 文档，框架自动扫描接口，批量生成工具定义（工具名、参数 json schema、描述），不用手动写每个 tool 声明。很多平台支持 OpenAPI 一键导入 API 作为 Agent 工具。

2.MCP Server Capability 声明

MCP Server 启动时主动`tools/list`上报所有可用工具、参数 schema；智能体自动发现，动态热加载新增工具，不需要硬编码工具列表。

**当前引入外部工具只支持输入 SSE 地址**
