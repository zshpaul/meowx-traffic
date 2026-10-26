# MeowX Traffic

适用于 MeowX 的非官方配套工具，与 MeowX 官方无隶属关系。

独立的 Windows 流量观察工具，使用 Go + MyGo + Windows WebView2。通过本机 MeowX 控制接口读取连接，不修改代理规则，不修改 MeowX，不读取 Clash Traffic 的账本。

## 使用

下载 Windows x64 发布包并解压，右键 `Install.ps1` 使用 PowerShell 运行；也可以双击 exe 便携使用。需要 MeowX 已运行，且用户配置中的 `patchClashConfig.external-controller` 为本机地址、`find-process-mode` 为 `always`。

默认自动从 `%APPDATA%\com.miaomiaowux\MeowX\config.json` 读取接口地址和密钥；自动模式下密钥不复制到新软件数据文件、不返回前端、不写入日志。也可在「设置」中手动填写控制接口地址和密钥（保存在本机 `settings.json`，页面不回显）。空版本号不会阻止连接：直接验证 `/connections` 的计数和连接结构。

界面布局参考 Clash Traffic：左侧导航，浅色 / 深色主题。

- 概览：总流量、上下行、连接数、代理 / 直连；按小时（单日）或按天的流量趋势；应用、访问地址、节点排行和路径分布。
- 应用 / 访问地址 / 节点：排行表格，点击一行在右侧查看趋势和关联的地址、应用、节点；搜索、CSV 导出。
- 实时连接：上下行速度、最近 5 分钟曲线、活动连接的速度、节点链、规则和时长；可按应用分组。
- 时间范围：今天 / 昨天 / 近 7 天 / 近 30 天 / 本月 / 自定义（含起止两日）；可只看代理或直连。
- 设置：连接方式与测试连接、开机自启、静默启动、主题、保留天数、打开数据目录、清空历史。
- 数据在 `%LOCALAPPDATA%\MeowX Traffic\history.json`；每 5 秒保存，正常退出再保存。按小时记录；0.3.0 之前的旧数据只有日期，不显示在小时趋势中。
- 关闭窗口继续在托盘采集；右键托盘可打开窗口、开关自启和静默启动、退出并保存。
- 默认静默启动。再次运行 exe 或使用开始菜单入口可打开窗口。安装脚本为当前用户启用登录自启，无需管理员。
- 不包含自动更新；升级前从托盘退出，然后重新运行新版安装脚本。历史独立保存，不随安装覆盖。

## 统计边界

仅统计 MeowX 控制器可见的流量；没有经过它的系统流量不在覆盖范围。初次连接及每次启动先建立基线，不追记此前数据。正常采样每秒一次；中断超过 10 秒或累计计数回退重新建立基线。短连接可能漏采；无法归属的内核增量不猜测分配到具体应用。内核差额可能因非原子快照短暂为负。

明细默认保留最近 30 天（设置中可改为 1–3650 天）；采集起点及核心累计增量保留。应用按原始进程名归类，不把 WebView2、Node 等辅助进程强行认定为某个软件。跨整点的一个采样增量归入采样结束时所在的小时。

这不是 Clash Traffic 的完整复制品：不包含订阅剩余额度、跨设备同步、已结束连接的逐条访问日志、辅助进程智能合并。界面只参考其布局与视觉，独立编写，没有复制其实现（Clash Traffic 仓库未附许可证）。

## 构建和测试

```powershell
.tools\go\bin\go.exe test ./...
$env:MEOWX_LIVE_TEST='1'
.tools\go\bin\go.exe test -v ./...
.tools\go\bin\go.exe build -ldflags '-H windowsgui' -o 'dist\MeowX Traffic.exe' .
```

Go 1.27.3，MyGo v0.4.8。Go 工具链是项目本地下载且经官方 SHA256 校验，不修改全局 PATH。

## 工作原理与研究

见 `docs/architecture.md`。REA 分析了 Clash Traffic 前端；后端机制通过固定版本源码审查验证。新程序独立实现，没有复制 Clash Traffic 的实现。

不能恢复 首次运行之前的按应用流量，也不能看到 HTTPS 下载内容的具体文件路径。

## 另一台电脑

支持 Windows x64，需要 Microsoft Edge WebView2 Runtime 和 MeowX。下载同一个发布包，按上方步骤安装。不要复制其他电脑的 MeowX 配置、密钥或流量历史。

在该电脑的 MeowX 持久设置中启用本机控制接口，例如 `127.0.0.1:9090`，设置自己的非空 secret，并启用 `find-process-mode: always`，然后重启 MeowX。不要把代理端口 7890 当控制接口端口。程序读取该电脑 `%APPDATA%\com.miaomiaowux\MeowX\config.json` 的 `patchClashConfig`；配置在其他位置时，在「设置 → MeowX 连接」改为手动填写。若 MeowX 未就绪会持续重试。

此应用不会替你修改 MeowX 配置。应用启动晚于 MeowX 或早于 MeowX 均可；断线期间不保证补计。各电脑分别统计，不会自动合并。

卸载：托盘关闭开机自启，退出程序，删除 `%LOCALAPPDATA%\Programs\MeowX Traffic` 及开始菜单快捷方式即可。历史数据默认保留。
