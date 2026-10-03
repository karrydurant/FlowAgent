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

### 以 Agent 调用工具实现搜索物品 A 为例，演示一下过程

阶段一：握手+工具列表发现(tools/list)

1.Agent(MCP Client) 拿到 MCP Server 的 SSE 地址，发起 GET/sse 建立 SSE 长连接

2.MCP Server 通过 SSE 推送消息，返回 messageEndpoint（POST 接口地址，用来下发指令）

3.Agent 通过 POST 向这个 endpoint 发送一条 MCP JSON-RPC 请求：tools/list。问：你有什么工具？把工具能力告诉我

4.MCP Server 返回工具清单

```json
{
  "tools":[
    {
      "name":"web_search",
      "description":"网页搜索工具，用于检索互联网信息",
      "inputSchema":{
        "type":"object",
        "properties":{"query":{"type":"string","description":"搜索关键词"}}
      }
    }
  ]
}
```

MCP Server 主动告诉 Agent：我有一个 web_search 工具，需要传入 query 字符串参数。

阶段二：大模型判断要不要调用工具，构造调用请求

1.用户提问：帮我搜索物品 A

2.大模型读到用户问题 + MCP 上报的工具描述（`web_search`的功能和参数 schema）

3.模型推理判断：我自己不知道物品 A 信息，需要调用`web_search`工具，参数`query="物品A"`

4.Agent（MCP Client）封装成 MCP JSON-RPC 调用消息 `tools/call`，POST 发送给 messageEndpoint

```json
{
  "jsonrpc":"2.0",
  "method":"tools/call",
  "params":{
    "name":"web_search",
    "arguments":{"query":"物品A"}
  }
}
```

阶段三：MCP Server 收到调用请求，真正执行工具逻辑

1.解析收到的`tools/call`请求，识别工具名`web_search`、参数`query=物品A`
   
2.执行内部业务代码：调用搜索后端 / 爬虫 / 浏览器 API，发起网络请求去搜索物品 A

3.拿到搜索结果

阶段四：MCP Server 把结果，通过 SSE 推回 Agent

1.搜索结果组装成 MCP 规定的返回消息

2.通过已经建立好的 SSE 长连接，推送给 Agent（支持分片流式返回，边搜边回）

3.Agent 拿到搜索结果，交给大模型，大模型基于搜索结果整理答案给用户
