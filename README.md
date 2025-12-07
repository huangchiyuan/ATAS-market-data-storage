# Market Data Storage System

> 一个高性能的市场数据接收、存储和读取系统  
> 支持从 ATAS 平台接收实时数据，存储到 DuckDB 数据库，并提供便捷的数据读取接口  
> 📧 联系方式：huangchiyuan@foxmail.com

---

## 📋 目录

- [功能特性](#功能特性)
- [系统架构](#系统架构)
- [快速开始](#快速开始)
- [C# 数据发送端](#c-数据发送端)
- [Python 数据接收端](#python-数据接收端)
- [数据读取](#数据读取)
- [配置说明](#配置说明)
- [常见问题](#常见问题)

---

## ✨ 功能特性

### 核心功能

1. **实时数据接收**
   - 从 ATAS 平台通过 UDP 接收 Tick 和 DOM 数据
   - 支持高频数据流（15,000+ 条/秒）
   - 自动处理数据流中断和重连

2. **高效数据存储**
   - 使用 DuckDB 列式数据库，高性能写入
   - 按日期自动分文件存储
   - 支持追加写入（同一天的数据可以多次追加）
   - 自动去重，防止重复数据

3. **便捷数据读取**
   - 支持单天数据读取
   - 支持日期范围数据读取（跨天连续）
   - 自动合并多天数据
   - 提供数据统计和摘要信息

### 技术特点

- ✅ **高性能**：队列缓冲 + 批量写入，支持高速回放（1000x）
- ✅ **数据完整性**：队列满时阻塞等待，确保数据不丢失
- ✅ **自动去重**：基于 `(symbol, exchange_time)` 自动去重
- ✅ **时区处理**：正确处理 UTC+0 时间，避免时区问题
- ✅ **优雅关闭**：确保所有数据都写入后再退出

---

## 🏗️ 系统架构

```
┌──────────────────────────────────────────────────────────────┐
│                  ATAS 交易平台 (C#)                           │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌───────────────┐                                           │
│  │ NFQE_Bridge_UDP │  (Indicator)                            │
│  │ 数据发送端      │                                           │
│  └───────┬───────┘                                           │
│          │ UDP:5555                                          │
└──────────┼───────────────────────────────────────────────────┘
           │
           ↓
┌──────────┴───────────────────────────────────────────────────┐
│             Python 数据接收和存储层                          │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌─────────────────────────────────────────────────────┐     │
│  │  UDP 数据接收 (ReceiverThread)                     │     │
│  │    • 接收 Tick / DOM 数据                           │     │
│  │    • 事件队列管理                                   │     │
│  └──────────────────┬──────────────────────────────────┘     │
│                     │                                        │
│                     ↓                                        │
│  ┌─────────────────────────────────────────────────────┐     │
│  │  数据库写入 (DbWriterThread)                        │     │
│  │    • 按日期分组数据                                 │     │
│  │    • 批量写入 DuckDB                                │     │
│  │    • 自动去重                                       │     │
│  └──────────────────┬──────────────────────────────────┘     │
│                     │                                        │
│                     ↓                                        │
│  ┌─────────────────────────────────────────────────────┐     │
│  │  DuckDB 数据库文件                                  │     │
│  │    market_data_YYYY-MM-DD.duckdb                   │     │
│  └─────────────────────────────────────────────────────┘     │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

---

## 🚀 快速开始

### 1. 环境准备

#### Python 端

```bash
# 安装依赖
pip install -r requirements.txt
```

#### C# 端（ATAS 平台）

1. **安装 ATAS Platform**（如果还没有安装）
2. **部署指标 DLL**（见下方详细说明）

### 2. 部署 C# 指标到 ATAS

#### 方法一：使用预编译的 DLL（推荐）

1. **复制 DLL 文件**：
   - 将 `NFQE_Bridge_UDP.dll` 复制到 ATAS 指标目录：
   ```
   C:\Users\Administrator\AppData\Roaming\ATAS\Indicators
   ```
   - 注意：如果使用其他用户，路径为 `C:\Users\<用户名>\AppData\Roaming\ATAS\Indicators`

2. **重启 ATAS 平台**（如果正在运行）

#### 方法二：从源代码编译

1. 使用 Visual Studio 2022 打开 `csharp/ATASDataGateway.sln`
2. 修改 `ATASDataGateway.csproj` 中的 ATAS DLL 引用路径（根据你的 ATAS 安装路径）
3. 编译项目（Release 模式）
4. 将生成的 `NFQE_Bridge_UDP.dll` 复制到 ATAS 指标目录（见方法一）

### 3. 启动 Python 数据接收程序

#### 方式一：存储到数据库（推荐）

```bash
python data_storage_module.py
```

程序启动后会显示：
```
📁 数据库文件夹: F:\量化交易系统\market_data_storage\market_data_db
🚀 启动数据存储模块...
🚀 [Receiver] 接收线程启动
🚀 [Writer] 写入线程启动
✅ 数据存储模块已启动
💡 按 Ctrl+C 停止（或等待30秒无数据自动停止）
[STATUS] Listening on 5555...
```

#### 方式二：实时显示 Demo（测试用）

```bash
python demo_realtime_receiver.py
```

这个 Demo 会实时显示接收到的数据，不存储到数据库，适合：
- 测试 ATAS 数据发送是否正常
- 查看实时数据格式
- 调试数据接收问题

### 4. 在 ATAS 中加载指标

1. **打开 ATAS 平台**
2. **打开图表**（ES/NQ/YM 等合约）
3. **添加指标**：
   - 右键点击图表 → "Indicators" → 找到 "NFQE DataPump V4.0 (Live Trading Optimized)"
   - 或直接搜索 "NFQE"
4. **配置参数**（在指标设置中）：
   - **Python IP**: `127.0.0.1`
   - **Python Port**: `5555`
   - **DOM Depth**: `15`（实际发送全部可见档位）

### 5. 验证数据接收

当 ATAS 开始发送数据后，Python 控制台应显示：
```
✅ [Writer] 数据库已连接: market_data_db/market_data_2025-12-07.duckdb
📅 [Writer] 开始写入日期: 2025-12-07 (Ticks: 1000, DOMs: 500)
[STATUS] Total: 1,500
[STATS] ✅ 队列: 0 | 接收: 1,500 (150/s) | 写入: 1,500 (150/s)
```

### 6. 停止数据存储

- **方式一**：按 `Ctrl+C`（优雅关闭，确保所有数据写入）
- **方式二**：等待 30 秒无数据自动停止

---

## 📡 C# 数据发送端

### 文件位置

- **预编译 DLL**: `NFQE_Bridge_UDP.dll`（可直接使用）
- **源代码**: `csharp/NFQE_Bridge_UDP.cs`
- **项目文件**: `csharp/ATASDataGateway.csproj`

### 部署说明（推荐使用预编译 DLL）

1. **复制 DLL 文件**：
   ```
   源文件: NFQE_Bridge_UDP.dll
   目标目录: C:\Users\Administrator\AppData\Roaming\ATAS\Indicators
   ```
   - 注意：如果使用其他用户，路径为 `C:\Users\<用户名>\AppData\Roaming\ATAS\Indicators`
   - 如果该目录不存在，需要先创建

2. **重启 ATAS 平台**（如果正在运行）

3. **验证部署**：
   - 在 ATAS 中打开图表
   - 添加指标时应该能看到 "NFQE DataPump V4.0 (Live Trading Optimized)"

### 从源代码编译（可选）

如果需要修改代码或重新编译：

1. **使用 Visual Studio 2022**：
   - 打开 `csharp/ATASDataGateway.sln`
   - 修改 `ATASDataGateway.csproj` 中的 ATAS DLL 引用路径（根据你的 ATAS 安装路径）
   - 编译项目（Release 模式）
   - 生成的 DLL 文件在 `bin/Release/net8.0-windows/NFQE_Bridge_UDP.dll`

2. **部署到 ATAS**：
   - 将生成的 `NFQE_Bridge_UDP.dll` 复制到 ATAS 指标目录（见上方部署说明）
   - 重启 ATAS 平台

### 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `PythonIP` | `127.0.0.1` | Python 监听地址 |
| `PythonPort` | `5555` | UDP 数据发送端口 |
| `DepthLevels` | `15` | DOM 深度（实际发送全部可见档位） |

### 数据格式

#### Tick 数据
```
T,Symbol,Price,Volume,Side,ExchangeTimeTicks
```
示例：`T,ES,6849.25,5,BUY,638456789012345678`

#### DOM 数据
```
D,Symbol,bids,asks,ExchangeTimeTicks
```
示例：`D,ES,6849.50@17|6849.25@28,6849.75@12|6850.00@20,638456789012345678`

#### 心跳数据
```
H,Symbol,LocalTimeTicks
```

---

## 🐍 Python 数据接收端

### 核心模块

#### `data_storage_module.py`

**主要类**：

- `StorageModule`：主程序类，管理接收和写入线程
- `ReceiverThread`：UDP 接收线程，接收数据并放入队列
- `DbWriterThread`：数据库写入线程，从队列消费数据并写入数据库
- `DataReader`：数据读取工具（基础版本）

**使用方法**：

```python
from data_storage_module import StorageModule

# 创建并启动模块
module = StorageModule()
module.start()

# 运行（阻塞）
module.run()

# 或手动停止
module.stop(graceful=True)
```

#### `demo_realtime_receiver.py`（实时接收 Demo）

**功能**：
- 实时接收和显示 Tick 和 DOM 数据
- 统计接收速率
- 显示最新价格和 DOM 快照

**使用方法**：

```bash
python demo_realtime_receiver.py
```

**输出示例**：
```
[TICK] 🟢 ES     | 价格:    6849.25 | 数量:        5 | 方向: BUY  | 时间: 14:30:15.123
[DOM]  ES     | Bid:    6849.25 | Ask:    6849.50 | Spread:   0.25 | 时间: 14:30:15.125
        Bids: 6849.25@17 | 6849.00@50 | 6848.75@30
        Asks: 6849.50@12 | 6849.75@20 | 6850.00@15
```

**适用场景**：
- 测试 ATAS 数据发送是否正常
- 查看实时数据格式
- 调试数据接收问题

### 配置参数

```python
# 核心配置
UDP_IP = "127.0.0.1"
UDP_PORT = 5555
DB_FOLDER = "market_data_db"
QUEUE_SIZE = 2000000  # 队列大小（2倍缓冲）
DB_BATCH_SIZE = 100000  # 批量写入大小
BATCH_TIMEOUT = 0.3  # 批次超时（秒）
AUTO_STOP_TIMEOUT = 30.0  # 自动停止超时（秒）
```

### 数据库结构

**Tick 表** (`ticks`)：
```sql
CREATE TABLE ticks (
    symbol VARCHAR,
    price DOUBLE,
    volume DOUBLE,
    side VARCHAR,
    exchange_time TIMESTAMP,
    recv_time TIMESTAMP
);
```

**DOM 表** (`depth`)：
```sql
CREATE TABLE depth (
    symbol VARCHAR,
    bids VARCHAR,
    asks VARCHAR,
    exchange_time TIMESTAMP,
    recv_time TIMESTAMP
);
```

---

## 📖 数据读取

### 使用 `data_reader_for_backtest.py`

```python
from data_reader_for_backtest import DataReaderForBacktest

# 创建读取器
reader = DataReaderForBacktest()

# 列出所有可用日期
dates = reader.list_available_dates()
print(f"可用日期: {dates}")

# 读取单天数据
df_ticks, df_depth = reader.load_single_day("2025-12-07", "ES")

# 读取日期范围数据
df_ticks, df_depth = reader.load_date_range("2025-12-01", "2025-12-07", "ES")

# 打印数据摘要
reader.print_summary()
```

### 数据格式

**Tick 数据** (`df_ticks`)：
- 索引：`exchange_time` (TIMESTAMP)
- 列：`price`, `volume`, `side`, `recv_time`

**DOM 数据** (`df_depth`)：
- 索引：`exchange_time` (TIMESTAMP)
- 列：`bids`, `asks`, `recv_time`
- `bids` 和 `asks` 为字符串格式：`"price@volume|price@volume|..."`

---

## ⚙️ 配置说明

### 性能优化

**队列大小**：
- 默认：2,000,000（2倍缓冲）
- 如果队列经常满载，可以增加此值

**批量写入**：
- `DB_BATCH_SIZE`：100,000（减少写入次数）
- `BATCH_TIMEOUT`：0.3 秒（更频繁刷新）

**自动停止**：
- `AUTO_STOP_TIMEOUT`：30 秒
- 如果 30 秒没有数据，自动停止

### 数据去重

系统自动基于 `(symbol, exchange_time)` 去重：
- 如果重复接收数据，只插入缺失的数据
- 不会产生重复记录

---

## ❓ 常见问题

### Q1: 队列使用率 100% 会造成数据丢失吗？

**A**: 不会。系统使用阻塞 `put(block=True)`，队列满时会等待，确保所有数据都能进入队列。

### Q2: 如果部分数据丢失，重复接收会发生什么？

**A**: 系统会自动去重：
- 已存在的数据：自动跳过
- 缺失的数据：自动插入
- 最终结果：完整的数据集

### Q3: 如何检查数据完整性？

**A**: 使用 `data_reader_for_backtest.py`：
```python
reader = DataReaderForBacktest()
reader.print_summary()  # 打印所有数据的摘要信息
```

### Q4: 数据存储在哪里？

**A**: 默认存储在 `market_data_db/` 文件夹中，按日期分文件：
```
market_data_db/
├── market_data_2025-12-01.duckdb
├── market_data_2025-12-02.duckdb
└── ...
```

### Q5: 如何修改数据库存储位置？

**A**: 修改 `data_storage_module.py` 中的 `DB_FOLDER` 变量：
```python
DB_FOLDER = "your_custom_folder"
```

### Q6: ATAS 中找不到指标怎么办？

**A**: 检查以下几点：
1. 确认 DLL 文件已复制到正确的目录：`C:\Users\<用户名>\AppData\Roaming\ATAS\Indicators`
2. 确认已重启 ATAS 平台
3. 在 ATAS 中搜索 "NFQE" 或 "DataPump"
4. 检查 DLL 文件是否与 ATAS 平台版本兼容

### Q7: 如何找到 ATAS 指标目录？

**A**: 
- **Windows**: `C:\Users\<用户名>\AppData\Roaming\ATAS\Indicators`
- 如果找不到，可以在 ATAS 中：
  1. 点击 "File" → "Settings"
  2. 查看 "Paths" 或 "Directories" 设置
  3. 找到 Indicators 目录路径

---

## 📄 许可证

详见 `LICENSE` 文件。

---

## 📞 联系方式

如有问题或建议，欢迎联系：

📧 **邮箱**：huangchiyuan@foxmail.com

---

**项目名称**：Market Data Storage System  
**版本**：v1.0  
**最后更新**：2025-12-07  
**维护状态**：✅ 活跃开发中  
**联系方式**：huangchiyuan@foxmail.com

---


