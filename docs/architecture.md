# clash-traffic 源码证据报告

分析日期：2026-10-26。源码根目录：`https://github.com/lufficc/clash-traffic/tree/1c2524923ab14de59068e106ae8c1dcaa4b993c8`。

Git HEAD：`1c2524923ab14de59068e106ae8c1dcaa4b993c8`。检查开始时该源码仓库 `git status --short` 无输出。以下所有 `相对文件:行号` 均相对于上述源码根目录，定位到该 SHA 的具体实现；不是相对于本报告所在目录。

范围：只读静态分析轮询、差分计数、重启续算、应用分组、历史存储与直连/代理分类。未联网、未启动程序、未执行测试、未安装依赖、未修改源码；唯一交付为本报告。不分析 REA，不设计或实现新软件。“推论”表示根据控制流得出的边界判断，不表示已完成运行复现。

## 1. 结论与影响

该程序是 Clash 控制器连接快照的统计客户端：读取 `GET /connections` 的连接累计字节和核心累计总量，而不是直接采集操作系统网卡或进程流量。因此，它能记录的是控制器所报告的流量，不能据此承诺覆盖绕过 Clash 的整机直连流量。证据：`internal/clash/client.go:105`、`internal/clash/client.go:206`、`core.go:297`。

它同时保存两类历史：按小时聚合的流量账本，以及按连接保存的本机访问日志。前者含漏采差额补偿，后者使用最后观测到的连接累计字节；二者不是同一口径，不能要求逐条相加必然一致。证据：`internal/collector/collector.go:250`、`internal/collector/collector.go:277`、`internal/collector/collector.go:343`、`internal/collector/collector.go:417`。

正常退出会保存计数和 checkpoint；异常退出通过 generation 检查避免沿用不匹配的计数基线，代价可能是漏计。短暂重启可以恢复核心差额余额，但长时间退出不保证补齐停机期流量。证据：`core.go:244`、`core.go:458`、`internal/collector/collector.go:185`、`internal/collector/collector.go:554`。

## 2. 轮询与实时展示

| 项目 | 实际行为 | 源码证据 |
| --- | --- | --- |
| 采样周期 | 常量为 1 秒，单个 `pollLoop` goroutine 通过 ticker 驱动 | `core.go:57`、`core.go:217` |
| 数据接口 | HTTP `GET /connections`；返回核心上传/下载累计值与当前连接数组 | `internal/clash/client.go:105`、`internal/clash/client.go:207` |
| 传输方式 | 支持 TCP HTTP、Unix socket 和 Windows named pipe；后两者使用自定义 HTTP transport | `internal/clash/client.go:26`、`internal/clash/client.go:120` |
| 请求失败 | 每次连接快照请求的 context 超时为 3 秒；连续失败 3 次后丢弃客户端，保留 collector 的计数状态 | `core.go:231`、`core.go:297` |
| 重新发现 | 连接失败后指数退避，从 1 秒开始，上限 15 秒 | `core.go:224`、`core.go:275` |
| 保存周期 | 同一轮询循环中检查距上次 flush 是否达到 30 秒；不是独立精确定时写盘线程 | `core.go:58`、`core.go:260` |
| 其他周期任务 | 每分钟刷新订阅；每 6 小时清理访问日志；成功采样后每 30 秒检查进程识别配置 | `core.go:264`、`core.go:268`、`core.go:312` |
| 实时事件 | 保存完整 live 快照后，广播最多 300 条连接；连接先按上下行速率之和降序，再按开始时间降序排序 | `core.go:411`、`internal/collector/collector.go:288` |
| 实时速率历史 | 仅在内存保留最近 300 个成功快照点；不严格等于 300 秒 | `core.go:59`、`core.go:414` |
| 汇总页刷新 | 当前时间范围的报告前端每 5 秒查询一次，另响应 data 事件 | `src/lib/hooks.ts:35`、`src/lib/hooks.ts:59` |

推论：1 秒是目标调度间隔，不是严格采样间隔。网络请求、发现、清理、flush 和配置检查均在采样循环内同步进行，慢操作会拉长相邻成功快照的间距。速率使用实际 `now-lastTick` 秒数计算，首次快照或非正时间间隔返回零速率。证据：`core.go:251`、`core.go:310`、`core.go:314`、`internal/collector/collector.go:209`、`internal/collector/collector.go:217`。

关闭全部窗口不会停止计数；程序退出后才调用 `Core.Stop`，取消后台任务并等待退出保存。证据：`main.go:69`、`main.go:76`、`core.go:168`。

## 3. 差分记账与短连接补偿

### 3.1 连接差分

连接状态用连接 `ID` 索引，不使用 PID。每次快照计算：

```text
du = max(connection.upload   - previouslySeenUpload,   0)
dd = max(connection.download - previouslySeenDownload, 0)
```

随后更新基线，将增量记入采样时刻所属小时，键为 `Hour + App + Proc + Host + Chain + Sub`。新发现且开始时间不早于 `countFrom` 的连接计 `Conns += 1`；早于统计起点的存量连接，首次只建立累计字节基线，不倒灌既往流量，也不计一次新连接。证据：`internal/collector/collector.go:174`、`internal/collector/collector.go:225`、`internal/collector/collector.go:232`、`internal/collector/collector.go:242`、`internal/collector/collector.go:413`。

实时连接的 `Up/Down` 是核心提供的连接全生命周期累计值，速率才用上述增量除以实际间隔；因此实时连接累计值不等于“本程序开始统计后记入账本的值”。证据：`internal/collector/collector.go:254`。

### 3.2 核心总量对账

除首次/重置快照外，上下行独立维护余额：

```text
newBalance = oldBalance + deltaCoreTotal - sumObservedConnectionDeltas
give       = min(newBalance, previousBalance)
若 give > 0：将 give 分摊入账，再从余额扣除
余额下界为 -(64 << 20) 字节
```

这让正差额至少经过两个连续快照的余额确认，减少核心总量与各连接计数读取时刻不一致造成的重复补偿；负余额用于抵消后续差额，不直接撤销已经入账的应用字节。证据：`internal/collector/collector.go:277`、`internal/collector/collector.go:301`。

分摊候选是上一轮与本轮刚消失的连接。每个方向的权重为该连接最后一次观测增量加 `1024`，剩余整数舍入量由最后一个候选承担；不存在候选时写入“短连接（未捕获）”，host 为“（未知）”，chain/sub 为空。补偿只增加字节，不增加连接数。证据：`internal/collector/collector.go:267`、`internal/collector/collector.go:278`、`internal/collector/collector.go:313`。

影响与边界：

- 在两次快照之间完成的短连接不会被直接看到；总量可能通过余额补入，但应用、域名、出口和真实连接数无法从差额恢复。若同期存在刚关闭的其他连接，差额可能被分配给这些连接。属于启发式归因。证据：`internal/collector/collector.go:303`、`internal/collector/collector.go:326`、`internal/collector/collector.go:332`。
- 正余额尚未确认、首次快照、核心重启、长时间恢复丢弃余额、负债截断等情况下，不能保证任意时刻账本增量严格等于核心增量。证据：`internal/collector/collector.go:176`、`internal/collector/collector.go:203`、`internal/collector/collector.go:277`、`internal/collector/collector.go:302`。
- 跨小时采样间隔和延迟补偿都记到当前采样小时，不按实际传输时刻拆分。证据：`internal/collector/collector.go:174`、`internal/collector/collector.go:250`、`internal/collector/collector.go:279`。

## 4. 重启、断线与持久化一致性

### 4.1 保存与加载顺序

数据目录由 `mygo.PathUserData` 获取；不应仅从本源码推断某个固定 Windows 绝对数据目录。初始化设置路径为 `settings.json`、checkpoint 为 `collector-state.json`、聚合根目录为 `data`、访问日志目录为 `sessions`。证据：`main.go:48`、`core.go:101`、`core.go:113`、`core.go:120`。

初始化先打开 store，再按 `store.Gen()` 加载 checkpoint。checkpoint 保存快照时间、generation、连接 ID 到已计累计值的映射、endpoint、核心总量、当前与上一轮余额，以及上一轮关闭连接的分摊信息；不会完整保存仍打开连接的所有 metadata，恢复后会重新建键。证据：`core.go:144`、`internal/collector/collector.go:478`、`internal/collector/collector.go:516`、`internal/collector/collector.go:557`、`internal/collector/collector.go:239`。

flush 顺序是 `sessions.Flush()` → `store.Flush()` → `collector.Save()`。访问日志失败只记录日志，不阻止后两步；store 失败则不保存 checkpoint。这不是跨三个文件系统对象的原子事务。证据：`core.go:458`。

store 有脏日数据时先提升并写入 `device.json` 的 generation，再写日文件；重启时取设备元数据和最新三个日期文件中最大的 generation。checkpoint generation 必须严格相等，否则整份 checkpoint 拒绝加载。证据：`internal/store/store.go:155`、`internal/store/store.go:169`、`internal/store/store.go:346`、`internal/collector/collector.go:554`。

日汇总和 checkpoint 使用同目录临时文件，写入后 `Sync`、关闭，再 `Rename`；非 Windows 还尝试同步目录。它增强单文件写入完整性，但不构成整个账本与 checkpoint 的事务。证据：`internal/store/store.go:895`、`internal/collector/collector.go:538`。

### 4.2 不同恢复情形

| 情形 | 判定与动作 | 证据 |
| --- | --- | --- |
| 核心累计值下降 | 任一方向低于先前值，视为核心重启；关闭旧 tracked session、清空连接表与余额，按首次快照处理 | `internal/collector/collector.go:176` |
| 同一 endpoint 断线后重连 | 设置 verify；存在旧 ID 重叠，或者间隔不超过 1 分钟，视为同一核心并继续总量差额 | `internal/collector/collector.go:157`、`internal/collector/collector.go:191` |
| 应用重启且 checkpoint 有效 | 更严格：间隔不超过 1 分钟，且有旧 ID 重叠或此前连接表为空，才延续核心总量余额 | `internal/collector/collector.go:199`、`internal/collector/collector.go:561` |
| 应用长时间关闭 | 放弃核心余额续算，但不等于清空全部连接基线；仍存在的已保存 ID 可按连接累计差分续计 | `internal/collector/collector.go:203`、`internal/collector/collector.go:228`、`internal/collector/collector.go:557` |
| checkpoint 后新开的连接 | 有效加载后 `countFrom` 回退到保存时刻；若重启时该连接仍可见，可计其已发生的累计字节 | `internal/collector/collector.go:232`、`internal/collector/collector.go:571` |
| endpoint 改变 | `Reset()` 清 started 与余额；并未清空 `prev`，不能概括为“所有连接状态全部清空” | `internal/collector/collector.go:147`、`internal/collector/collector.go:159` |
| checkpoint 缺失、损坏或代数不符 | 初始化记录非缺失错误并继续；新 collector 对早于本次统计起点的连接只建立基线，可能漏计 | `core.go:144`、`internal/collector/collector.go:139`、`internal/collector/collector.go:232`、`internal/collector/collector.go:545` |

推论：同一核心识别是 endpoint、时间间隔、连接 ID 重叠与累计量回退的启发式组合，不是核心启动 UUID。快速核心重启后总量已超过旧值、或者跨核心复用连接 ID 等情形，并没有绝对身份保证。`Reset()` 保留 `prev` 也意味着切换 endpoint 时 ID 冲突可能沿用旧基线/旧归属。证据：`internal/collector/collector.go:147`、`internal/collector/collector.go:176`、`internal/collector/collector.go:199`、`internal/collector/collector.go:228`。

异常退出可能丢失自上次成功 flush 以来的内存数据；正常目标周期是 30 秒，但阻塞或写盘失败会使实际窗口更长。退出处理会保存当时仍打开的 session 后执行 flush。证据：`core.go:244`、`core.go:260`、`core.go:458`。

## 5. 应用分组与归属

应用身份来自控制器 metadata 的 `process` 和 `processPath`，没有在该结构中保存 PID。`procname.App()` 的规则为：

1. 路径含 macOS `.app/` 或以 `.app` 结尾时，使用最外层 bundle 名，因此 helper 可合并进父应用。
2. 其余情况优先使用 `process`；为空才使用路径 basename。
3. 去掉结尾 `.exe` 或 `.EXE`。
4. 名字符合版本号正则且有路径时，向上找非版本、非通用目录名作为应用名。

证据：`internal/clash/client.go:75`、`internal/procname/procname.go:11`、`internal/procname/procname.go:23`、`internal/procname/procname.go:45`、`internal/procname/procname.go:71`。

仍无法识别应用时，若来源 IP 属于其他 LAN 主机则使用“局域网 <IP>”，否则“未识别进程”。LAN 检查排除回环、未指定地址、`198.18.0.0/15` 与本机接口地址，只接受私网或链路本地单播；本机地址列表每分钟刷新。证据：`internal/collector/collector.go:353`、`internal/collector/collector.go:445`、`internal/collector/collector.go:461`。

账本内部仍保留 `Proc` 维度，但应用榜单按 `App` 名称累加；路径不是聚合键，每天每个 app 只保存首次遇到的非空路径用于展示。推论：同名不同安装目录的程序会合并，不是可执行文件唯一身份分组；也不是通用的 Windows 父子进程树归并。证据：`internal/store/store.go:61`、`internal/store/store.go:282`、`internal/store/query.go:373`、`internal/procname/procname.go:29`。

应用、host、chain、sub 首次建键后即固定；后续同 ID metadata 变完整或出口信息变化不会自动重建归属。host 优先级为 `Host` → `SniffHost` → `DestinationIP` → “（未知）”。证据：`internal/collector/collector.go:239`、`internal/collector/collector.go:348`。

默认 `ForceProcess=true`，运行时每 30 秒检查 `find-process-mode`，必要时通过 `PATCH /configs` 设为 `always`。因此源程序本身并非只读监视器；本次分析没有启动它，也没有执行这些请求。进程识别最终仍依赖核心返回的 metadata。证据：`settings.go:82`、`core.go:312`、`core.go:368`、`internal/clash/client.go:229`。

## 6. 历史存储与查询口径

### 6.1 小时聚合账本

- 目录结构为 `data/<device-id>/device.json` 和 `data/<device-id>/YYYY-MM-DD.json.gz`；日文件按 UTC 日期分区。版本为 2，字符串字典压缩维度，行保存日内小时、app/proc/host/chain/sub 索引和上传/下载/连接数。证据：`internal/store/store.go:1`、`internal/store/store.go:35`、`internal/store/store.go:134`、`internal/store/store.go:494`、`internal/store/store.go:536`。
- 日文件是完整重写，而不是每秒追加；同一日重启后先读取原有行再累加。读取失败会尝试移到 `.unreadable-<timestamp>` 后重新统计，无法移开则禁止保存该日新数据。证据：`internal/store/store.go:290`、`internal/store/store.go:356`、`internal/store/store.go:546`。
- 查询是小时桶口径：纳入桶起点位于查询范围内的小时，不按任意分钟或秒数比例切分。超过 48 小时默认展示按本地日聚合，但底层仍为小时数据。证据：`internal/store/query.go:239`、`internal/store/query.go:265`、`internal/store/query.go:279`。
- `SessionDays` 清理的是访问日志；这里的 flush 删除的是旧日内存缓存，并不删除旧日聚合文件。所检查调用链未见按该配置自动过期小时汇总的逻辑。证据：`core.go:270`、`core.go:641`、`internal/store/store.go:365`。
- 各榜单忽略自身维度筛选以保留可选列表；例如 app 榜仍列出其他 app，不能把它的合计直接当作当前 app 筛选后的报告总计。证据：`internal/store/query.go:15`、`internal/store/query.go:324`、`internal/store/query.go:364`、`internal/store/query.go:373`。

### 6.2 连接访问日志

- 本机 `sessions/YYYY-MM-DD.jsonl.gz`，按 session 最后可见时间所在 UTC 日分区；每批追加一个 gzip member，内容为 JSON lines，不重写整个正常日文件。证据：`internal/sessions/sessions.go:1`、`internal/sessions/sessions.go:142`、`internal/sessions/sessions.go:181`、`internal/sessions/sessions.go:218`。
- 连接从快照消失时记录；`End` 是最后一次看到的时间，不是真正关闭时间。记录字节是最后观察的累计值，差额补偿只写入 store，不回填日志。证据：`internal/collector/collector.go:244`、`internal/collector/collector.go:267`、`internal/collector/collector.go:343`、`internal/collector/collector.go:417`。
- 正常退出会记录 `Open=true` 的连接；查询优先采用当前 live，关闭记录覆盖退出时记录；仅留下退出时记录且目前不再 live 的连接标为 `Cut=true`，表示未知时刻结束。证据：`core.go:244`、`core.go:665`、`internal/sessions/sessions.go:547`、`internal/sessions/sessions.go:574`。
- 范围查询选择与区间重叠的 session，然后累加其完整累计字节，并不切割到区间内；默认每页 200，按开始时间由新到旧排序。证据：`internal/sessions/sessions.go:454`、`internal/sessions/sessions.go:477`、`internal/sessions/sessions.go:488`、`internal/sessions/sessions.go:512`、`internal/sessions/sessions.go:600`。
- 默认保留 30 天，`0` 表示保留全部；按 UTC 日期阈值删除早于 cutoff 的文件，并非严格滚动 30×24 小时逐条过期。每 6 小时以及更改配置时清理。证据：`settings.go:57`、`settings.go:90`、`internal/sessions/sessions.go:420`、`core.go:268`、`core.go:641`。
- 首次追加前会检查并修复损坏日志，保留 `.damaged-*` 副本；失败写入尝试回退文件长度并重试，读取时按 ID 与 Open 状态去重。证据：`internal/sessions/sessions.go:181`、`internal/sessions/sessions.go:248`、`internal/sessions/sessions.go:359`。
- 已解码缓存目标上限为 400,000 条；更大的单日仍可能被读入但不缓存，所以它不是整个查询进程的硬内存上限。追加完成使用文件 `Close()`，没有和聚合文件相同的显式 `Sync()` 流程。证据：`internal/sessions/sessions.go:107`、`internal/sessions/sessions.go:331`、`internal/sessions/sessions.go:349`、`internal/sessions/sessions.go:238`。

日志目录与共享的 store 根目录不同；同步器和 mesh 使用 store 根目录，访问日志仅保存在本机。证据：`core.go:113`、`core.go:119`、`core.go:120`、`core.go:132`、`internal/sessions/sessions.go:10`。

## 7. DIRECT / proxy / reject / unknown

分类依据为 `Connections[].Chains[0]`，源码将其解释为最终出口，而不是根据 IP、规则名称、进程或连接是否经过 TUN 判定。完整 chains 反转后拼接用于展示。证据：`internal/collector/collector.go:371`、`internal/collector/collector.go:399`。

`RouteOf` 先 trim 并转大写，然后映射：

| 最终出口名 | 分类 |
| --- | --- |
| `DIRECT` | `direct` |
| `REJECT`、`REJECT-DROP` | `reject` |
| 空字符串 | `unknown` |
| 其他任意非空名字 | `proxy` |

证据：`internal/store/query.go:57`。

只有 proxy 连接才设置订阅：取 `ProviderChains` 首个非空值，缺失则用当前订阅回调，再缺失则“未知订阅”。当前订阅来自每分钟读取的 Clash Verge profiles。证据：`internal/collector/collector.go:375`、`core.go:264`、`core.go:347`。

不能把“无订阅”严格等同于 DIRECT：reject 与 unknown 也可没有订阅，查询 `NoSub` 实际按 sub 是否为空筛选。无法归因的短连接桶出口为空，属于 unknown；而分摊到已关闭连接的补偿则继承其出口和订阅。证据：`internal/store/query.go:333`、`internal/collector/collector.go:327`、`internal/collector/collector.go:343`。

推论：该分类是出口名字分类，不验证实际代理协议或节点类型。非标准命名的直连出口也可能落入 proxy。更重要的是，“DIRECT 统计”仅指核心报告的 DIRECT 连接，不代表绕过核心的系统直连。证据：`internal/store/query.go:57`、`internal/clash/client.go:207`。

## 8. 已核对的测试证据与验证边界

静态阅读到的测试覆盖了代数不匹配拒绝恢复、快速重启的余额抵消、关闭连接尾量跨重启补偿、长时间重启舍弃余额、同 endpoint 重连和切换 endpoint 等预期。对应位置：

- `internal/collector/collector_test.go:210`：`TestCheckpoint`。
- `internal/collector/collector_test.go:258`：`TestSkewAcrossRestart`。
- `internal/collector/collector_test.go:279`：`TestClosedTailAcrossRestart`。
- `internal/collector/collector_test.go:307`：`TestResume`。
- `internal/collector/collector_test.go:344`：`TestFirstConnectionAfterRestart`。

这些是已存在的测试代码证据，不是本次通过的测试结果。本次仅检查本地源码、行号和 Git 状态；没有用真实控制器验证采样准确率、进程识别成功率、Windows 断电恢复或长时间运行的资源占用。报告中的机制结论可由所引实现核对，涉及极端条件的风险明确按推论处理。
