# 喵汪还魂 · 抖音云接入包

2026-09-28；AppId `tt19616fdc8719e41710`。本目录是待联调的容器服务，尚未部署到抖音云。用户选择抖音云后，原 V40 推荐的指令直推路线已停止采用。

## 实现范围

- `/start_game` 通过抖音云内网 OpenAPI 开启评论、点赞、礼物三类任务，业务失败返回失败。
- `/live_data_callback` 校验来源与字段，将每条消息单独封装，原始礼物数量和测试标记不变。
- Redis Lua 原子去重、入队；满队列返回失败；重试从队头继续。网关明确接收才移除队头。
- `/websocket_callback` 接收官方连接生命周期通知，不允许客户端伪造礼物或上传积分改变榜单。
- Unity `DouyinCloudBridge` 使用实际安装的官方 SDK API，等待可信 RoomInfo，连接 `/websocket_callback`，开局后调用 `/start_game`。
- 主线程有界队列按帧处理；本地已排队的上一局消息不进入下一局；礼物演出可见并经过帧末后才调用 SDK ReportAck。

## 部署前必须取得的配置

| 配置 | 来源 |
|---|---|
| EnvId、ServiceId | 当前应用的抖音云后台；写入客户端 Resources/DouyinCloudSettings.json |
| REDIS_URL | 抖音云 Redis 组件连接信息，仅作为服务端环境变量注入，不写进仓库 |
| DY_ENV | `dev` 或 `prod`，两者使用独立 Redis 命名空间，建议独立实例 |
| DY_CALLBACK_SOURCE | 控制台/真实内部回调确认的 x-tt-source 值，未猜测或填入假值 |
| DY_INTERNAL_CALLBACK_CONFIRMED | 云控制台确实已限制回调为内网后设 `1` |
| PORT | 容器监听端口，默认 `8000` |

云端回调 `/live_data_callback` 必须仅允许平台内网调用，不能加入公网授权路径。客户端入口只允许经过抖音云鉴权网关；HTTP 请求头自身不能证明身份。禁止绕过网关暴露容器原始端口。代码开关不能替代云端网络隔离。

在本目录使用 Dockerfile 构建容器；Redis 必须开启持久化并采用 noeviction。环境变量含连接密码时只使用后台密钥配置。客户端不存 AppSecret，也不手工写入启动 Token。

Unity 菜单 `Tools / MiaoWang / Douyin Cloud Configuration` 可保存公开 ID。当前 enabled=false，避免将缺配置的原型误标为正式连接。正式模式固定 IsDebug=false，启动凭证由直播伴侣/云启动提供。

## 尚未完成的联调与生产条件

本地验证记录：Unity 编译通过且 officialSdkLoaded=true；客户端协议/路由 36 项检查通过；独立 Windows 包构建前共 169 项检查通过；云服务 15 项单元测试通过。新包本地模拟运行完成（6 玩家、猫到 -100 终点胜利、狗失败）。隐藏窗口截图在新旧版本中均为黑屏，不能据此认定美术画面验收通过，尚需可见窗口检查。以上均不是真实直播数据验收。

1. 浏览器控制连接超时，云服务、Redis、回调和权限状态均未核实，也没有创建任何计费资源。
2. 当前网关推送成功判据严格要求响应 `err_no=0`。需要实际网关响应/完整官方协议核实；未知格式保留待发消息并重试，不把 HTTP 200 自动当履约。
3. Redis Lua 测试使用 fakeredis+Lua，不能代替真实抖音云 Redis 联调。当前全局 FIFO 可被某个失败房间阻塞，上线多直播间前需要按房间分区和死信人工处理策略。
4. 云队列只保证持久化到网关接收边界，不等于客户端消费确认。客户端去重与待履约记录仍为进程内；程序重启后的恢复、平台失败消息补偿、过期消息与跨局归属仍需补齐。
5. 非游戏阶段收到的消息不消费、不报成功履约。还需联调停推/恢复、未选队送礼的产品处理；不能先收礼后静默丢弃并上线运营。
6. test 标记保留，正式路由拒绝测试数据。独立审核测试局、测试贡献隔离和测试履约流程仍待联调。
7. 周/月榜目前仍是本地榜；本容器未实现服务器权威榜单，不接收客户端任意上报分数。阵营 group_id、对局同步、真实头像和正式礼物配置待后台确认。
8. 需要通过直播伴侣/官方测试工具完成真实评论、点赞、六礼物、断线重连、可见演出与履约后台的全链路验收，再启用正式配置。

## 官方来源

- https://developer.open-douyin.com/docs/resource/zh-CN/interaction/develop/unity-sdk/unity-sdk-access
- https://partner.open-douyin.com/docs/resource/zh-CN/interaction/develop/douyincloud/guide
- https://developer.open-douyin.com/docs/resource/zh-CN/interaction/develop/server/live-room-scope/data-open/data-open-desc
- SDK 原始包来源、版本和 SHA-256 见 Docs/Checks/liveopensdk-source.json。

SDK Vendor 目录保留官方版权文件。未修改 SDK 源文件；依赖通过 Unity Package Manager API 安装。

