# Voxtype 分叉维护与上游同步指南

> 适用仓库：[`dwdcth/voxtype`](https://github.com/dwdcth/voxtype)（分支：`dev`）  
> 上游仓库：[`peteonrails/voxtype`](https://github.com/peteonrails/voxtype)  
> 归档说明：针对微信输入法纯 Python 离线引擎接入所做的通用流式 WebSocket 客户端 (`WsStream`) 维护手册。

---

## 一、架构拓扑与交互关系

```
┌────────────────────────────────────────────────────────┐
│  voxtype 客户端 (Rust, 分叉版 dwdcth/voxtype)          │
│  ├─ 监听全局按键: KEY_F23 (或自定义热键)               │
│  ├─ 麦克风录音: cpal 16kHz mono (f32/s16le)           │
│  ├─ 转写引擎: TranscriptionEngine::WsStream           │
│  │   └─ src/transcribe/generic_ws.rs                  │
│  │       ├─ impl StreamingTranscriber (长连接 WS 流式) │
│  │       └─ impl Transcriber (批式 HTTP/WS 回退)      │
│  └─ 文本上屏: Paste (Ctrl+V) / wtype / dotool          │
└────────────────────────────────────────────────────────┘
                    ▲                  │
     JSON 实时 Partial/Final           │ Binary 音频帧
                    │                  ▼
┌────────────────────────────────────────────────────────┐
│  wetype-voice 服务端 (Python, vtype-linux-archive)     │
│  ├─ 端点: ws://127.0.0.1:8090/v1/audio/stream         │
│  ├─ 兼容: POST http://127.0.0.1:8090/v1/audio/trans...│
│  ├─ 声学: 微信 Conformer 16层 (6 Emformer + 10 Trans) │
│  ├─ 解码: 极速 CTC Prefix Beam Search + nlrgpt 重排    │
│  ├─ 降噪: mg3 常用字过滤 (剔除"蒴"等吸气杂音)          │
│  ├─ 纠偏: user_replacements.txt + user_hotwords.txt   │
│  └─ 标点: Electra 12层全句双向标点 + A一A 叠词保护    │
└────────────────────────────────────────────────────────┘
```

---

## 二、代码修改点全景图（Touchpoints Map）

分叉版本的修改遵循**最小侵入原则**（总共仅新增 2 个独立文件，其余 10 个文件仅各添加数行枚举匹配）：

| 序号 | 文件路径 | 类型 | 核心改动说明 |
|:---:|---|:---:|---|
| 1 | `src/transcribe/generic_ws.rs` | **新增** | 通用 WebSocket 客户端核心实现，负责协议握手、Binary 采样发送与 `Partial`/`Final` 解析。 |
| 2 | `src/config/engines/ws_stream.rs` | **新增** | `WsStreamConfig` 配置结构体定义（endpoint, encoding, language, sample_rate）。 |
| 3 | `src/config/engines/mod.rs` | 修改 | `TranscriptionEngine` 枚举新增 `WsStream` 变体；注册并重新导出 `ws_stream` 模块。 |
| 4 | `src/config/mod.rs` | 修改 | 重新导出 `WsStreamConfig` 供全局使用。 |
| 5 | `src/config/root.rs` | 修改 | `Config` 结构体增加 `pub ws_stream: Option<WsStreamConfig>` 字段；`Default` 初始化赋 `None`。 |
| 6 | `src/transcribe/mod.rs` | 修改 | 注册 `pub mod generic_ws;`；在 `create_transcriber` 工厂函数的 `match` 增加 `WsStream` 分支。 |
| 7 | `src/setup/variant_check.rs` | 修改 | 在 `required_feature` 中将 `WsStream` 归入内置无条件支持列表（避免启动时提示 feature 缺失告警）。 |
| 8 | `src/daemon.rs` | 修改 | 在状态更新与后台模型准备逻辑的 `match self.config.engine` 中增加 `WsStream` 分支。 |
| 9 | `src/output/mod.rs` | 修改 | 在托盘/状态输出的引擎图标 `match` 中增加 `WsStream => "\u{1F3A4}"` (🎤)。 |
| 10 | `src/vad/mod.rs` | 修改 | 在 VAD 引擎绑定的 `match` 中归入 `Energy` 分支。 |
| 11 | `src/config_set.rs` | 修改 | 命令行配置检查 `match` 中增加 `WsStream => true`。 |
| 12 | `src/tui/app.rs` | 修改 | TUI 配置面板跳过无本地模型文件的探测 `WsStream => return None`。 |

> **提示**：如果上游未来重构了某些辅助模块，只要核心的 `src/transcribe/generic_ws.rs` 存在，按上表在对应的 `match` 块补齐 `WsStream` 即可完成适配。

---

## 三、上游更新与同步流程（Git Rebase 工作流）

当官方仓库 `peteonrails/voxtype` 发布新版本或重要修复时，按以下步骤同步：

### 1. 确保工作区干净并拉取上游最新分支
```bash
cd /home/banshan/项目/forks/voxtype

# 检查当前分支状态
git status

# 拉取上游最新代码
git fetch upstream
```

### 2. 变基（Rebase）到上游主干
```bash
# 切换到 dev 分支（或 main 分支，依据 fork 习惯）
git checkout dev

# 基于上游 dev（或 main）变基
git rebase upstream/dev
```

### 3. 处理可能出现的代码冲突
如果遇到冲突，通常仅出现在 `match` 块行号变动或新加了引擎变体：
- 打开冲突文件，搜索 `<<<<<<<`。
- 保留上游新特性的同时，确保我们的 `TranscriptionEngine::WsStream` 变体依然存在于 `match` 块中。
- 解决完所有冲突后继续：
```bash
git add -A
git rebase --continue
```

### 4. 编译校验与测试
```bash
# 快速类型检查 (耗时约 5~10 秒)
cargo check

# 完整 release 构建 (关闭非必要特性，耗时约 2~3 分钟)
cargo build --release --no-default-features
```

### 5. 强制推送到您的个人 fork 仓库
```bash
# 变基后需要强制推送到个人远程
git push origin dev --force-with-lease
```

---

## 四、编译与构建最佳实践

### 1. 国内网络加速配置
确保 `~/.cargo/config.toml` 配置了国内 sparse 镜像源：
```toml
[source.crates-io]
replace-with = "ustc"

[source.ustc]
registry = "sparse+https://mirrors.ustc.edu.cn/crates.io-index/"
```

### 2. 规避重量级/耗时依赖
默认的 `Cargo.toml` 中 `openvino-genai` 的 git patch 会尝试克隆体积庞大的 `protocolbuffers/protobuf` 递归子模块。在我们的最小化构建中已将其屏蔽：
- 推荐构建命令：
  ```bash
  cargo build --release --no-default-features
  ```
- 这样会跳过 OpenVINO、CUDA、ONNX Runtime 等百兆编译依赖，仅构建基础语音管线与我们的 WebSocket 引擎，编译速度从几十分钟缩短至 **2 分钟左右**。

### 3. 安装与生效
编译完成后，系统全局使用的二进制通过软链接直接指向目标二进制，无需重复拷贝：
```bash
ln -sf /home/banshan/项目/forks/voxtype/target/release/voxtype ~/.local/bin/voxtype

# 验证当前版本号
voxtype --version
```

---

## 五、未来向上游提交 PR 的备忘路线

若未来打算将通用 WebSocket 流式协议合入官方上游，建议按如下规范整理：

1. **协议说明文档**：
   将 `vtype-linux-archive/docs/WS_PROTOCOL.md` 放置到 voxtype 的 `docs/WS_PROTOCOL.md`，作为通用协议规范供社区参考。
2. **Feature Gate 支持**：
   在 `Cargo.toml` 中增加一个 `ws-stream = ["dep:tokio-tungstenite"]` 的特性门控（当前 `tokio-tungstenite` 已被 soniox 使用），使上游可以选择性打包。
3. **提交 PR**：
   ```bash
   gh pr create --repo peteonrails/voxtype \
     --base dev \
     --head dwdcth:dev \
     --title "feat: add generic WebSocket streaming STT engine (WsStream)" \
     --body "Enables connecting to local custom STT engines via a minimal WebSocket streaming protocol. Spec: docs/WS_PROTOCOL.md."
   ```

---

## 六、微信离线语音服务端（Python）的日常维护

### 1. 服务启停与状态检查
```bash
# 查看后台运行状态
ps aux | grep -E "server\.py|voxtype daemon"

# 检查服务健康度
curl http://127.0.0.1:8090/health
# 期望返回: {"status":"ok","engine":"wetype-voice"}

# 重启服务端
pkill -f "engine/server.py"
cd /home/banshan/项目/vtype-linux-archive
nohup python3 engine/server.py --port 8090 > /tmp/ws_server.log 2>&1 &
```

### 2. 热词与术语自适应优化
当您发现某些专业术语或新词识别不够准时，直接在配置文件中添加，无需重新训练或重新编译：
- **添加专业术语**：在 `models/asr_weights/user_hotwords.txt` 尾部加一行词汇（如 `Kubernetes`、`向量数据库`）。
- **同近音混淆修正**：在 `models/asr_weights/user_replacements.txt` 尾部加一行映射（格式：`错词 正确词`，如 `利息 逆向`）。
- 改动后重启 `server.py` 即刻生效。

### 3. 麦克风音频健康度维护
如果在不同环境下（如使用耳机麦克风或外接麦克风）出现辅音吞字或识别飘忽：
- 运行 `pactl list sources` 查看当前输入设备。
- 保证麦克风输入增益在 **50% ~ 70%**（-12dB ~ -6dB）之间，留出 Headroom 空间，防止声音过载削顶导致清辅音谐波失真。
