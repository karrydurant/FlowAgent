# A2aController

## 定位

所有外部请求 —— 不管是前端网页上点了个 "发起协商" 按钮，还是工作流引擎跑到一个委派节点要派活，还是另一台机器上的实例喊你 "你有新消息了"—— 都先打到这个 Controller，然后它把活分发给后面的四个业务类。

Controller 自己不做任何业务逻辑，它只做三件事：解析参数、校验、调下面的业务类、把结果包成统一的Result返回

## 四个业务类

SessionManager：管会话的生命周期

PeerDispatcher：管跨实例投递

DelegationManager：管单次任务委派


