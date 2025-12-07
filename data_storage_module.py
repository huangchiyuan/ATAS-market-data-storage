"""
数据接收存储模块
================

功能：
1. 从UDP接收市场数据（Tick和DOM）
2. 按日期分文件存储到DuckDB数据库
3. 支持追加写入（同一天的数据可以多次追加）
4. 数据库文件存放在单独的文件夹中
5. 支持跨天连续读取（回溯时可以读取多天的数据）

使用方式：
    python data_storage_module.py
"""

import socket
import duckdb
import datetime
import time
import os
import threading
import queue
from typing import Optional, Dict, Any, List
from pathlib import Path

# --- 核心配置 ---
UDP_IP = "127.0.0.1"
UDP_PORT = 5555
DB_FOLDER = "market_data_db"  # 数据库文件夹
QUEUE_SIZE = 2000000  # 增加队列大小，提供更大缓冲（2倍）
DB_BATCH_SIZE = 100000  # 增加批次大小，减少写入次数（2倍）
BATCH_TIMEOUT = 0.3  # 减少超时，更频繁刷新（适应高速回放）
SOCKET_TIMEOUT = 2.0
AUTO_STOP_TIMEOUT = 30.0  # 自动停止超时（秒）：如果30秒没有数据，自动停止
AUTO_STOP_TIMEOUT = 30.0  # 自动停止超时（秒）：如果30秒没有数据，自动停止

# --- C# Ticks 工具 ---
TICKS_AT_EPOCH = 621355968000000000


def ticks_to_datetime_us(ticks_str: str) -> int:
    """将 C# ticks 转换为微秒时间戳"""
    try:
        ticks = int(ticks_str)
        microseconds = (ticks - TICKS_AT_EPOCH) // 10
        return microseconds
    except:
        return 0


def ticks_to_full_datetime(ticks_str: str) -> datetime.datetime:
    """将 C# ticks 转换为完整的 datetime 对象"""
    try:
        microseconds = int(ticks_str) / 10
        return datetime.datetime(1, 1, 1) + datetime.timedelta(microseconds=microseconds)
    except:
        return datetime.datetime.now()


def ensure_db_folder():
    """确保数据库文件夹存在"""
    Path(DB_FOLDER).mkdir(parents=True, exist_ok=True)
    return DB_FOLDER


def get_db_file_path(date_str: str) -> str:
    """获取指定日期的数据库文件路径"""
    db_folder = ensure_db_folder()
    return os.path.join(db_folder, f"market_data_{date_str}.duckdb")


# =======================================================================
# 数据库写入线程 (消费者)
# =======================================================================
class DbWriterThread(threading.Thread):
    def __init__(self, data_queue: queue.Queue, status_callback=None):
        super().__init__(daemon=True)
        self.data_queue = data_queue
        self.status_callback = status_callback
        self.running = True
        self.connections: Dict[str, duckdb.DuckDBPyConnection] = {}  # 按日期存储连接
        self.current_date: Optional[str] = None
        self.total_written = 0
        self.buffer: List[Dict[str, Any]] = []
        self.last_flush_time = time.time()
        self.last_logged_date: Optional[str] = None  # 上次打印日志的日期，用于减少重复日志

    def get_connection(self, date_str: str) -> duckdb.DuckDBPyConnection:
        """获取或创建指定日期的数据库连接"""
        if date_str not in self.connections:
            db_file = get_db_file_path(date_str)
            conn = duckdb.connect(db_file)
            
            # 创建表（如果不存在）
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ticks (
                    symbol VARCHAR, 
                    price DOUBLE, 
                    volume DOUBLE, 
                    side VARCHAR, 
                    exchange_time TIMESTAMP, 
                    recv_time TIMESTAMP
                );
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS depth (
                    symbol VARCHAR, 
                    bids VARCHAR, 
                    asks VARCHAR, 
                    exchange_time TIMESTAMP, 
                    recv_time TIMESTAMP
                );
            """)
            
            # 创建唯一索引以防止重复数据（基于 symbol + exchange_time）
            # 这样重复接收数据时不会产生重复记录
            try:
                # 先创建普通索引
                conn.execute("CREATE INDEX IF NOT EXISTS idx_ticks_time ON ticks(exchange_time);")
                # 创建唯一索引（如果不存在）
                # 注意：DuckDB不支持直接创建唯一索引，我们需要在插入时使用去重逻辑
            except:
                pass
            try:
                conn.execute("CREATE INDEX IF NOT EXISTS idx_depth_time ON depth(exchange_time);")
            except:
                pass
            
            self.connections[date_str] = conn
            print(f"✅ [Writer] 数据库已连接: {db_file}")
            if self.status_callback:
                self.status_callback(f"Writing to {db_file}", "green")
        
        return self.connections[date_str]

    def run(self):
        """主循环：从队列消费数据并写入数据库"""
        print("🚀 [Writer] 写入线程启动")

        while self.running or not self.data_queue.empty():
            try:
                # 如果不再运行，使用更短的超时时间，加快处理
                timeout = 0.5 if not self.running else 1.0
                item = self.data_queue.get(timeout=timeout)

                # 处理初始化消息
                if isinstance(item, dict) and 'init' in item:
                    if not self.current_date:
                        dt = ticks_to_full_datetime(item['init'])
                        self.current_date = dt.strftime("%Y-%m-%d")
                        # 预创建连接（但不强制切换）
                        self.get_connection(self.current_date)
                    continue

                # 直接添加到缓冲区，不在这里检查日期变化
                # 日期分组和切换在 flush() 中统一处理，避免频繁切换
                self.buffer.append(item)

                # 批量写入或超时写入
                now = time.time()
                # 如果不再运行，更频繁地刷新（加快关闭速度）
                batch_timeout = BATCH_TIMEOUT * 0.5 if not self.running else BATCH_TIMEOUT
                if len(self.buffer) >= DB_BATCH_SIZE or (now - self.last_flush_time) >= batch_timeout:
                    self.flush()

            except queue.Empty:
                # 如果队列为空且不再运行，刷新缓冲区
                if self.buffer and not self.running:
                    self.flush()
                # 如果缓冲区有数据但超时，也刷新（适应高速回放）
                elif self.buffer and (time.time() - self.last_flush_time) >= BATCH_TIMEOUT:
                    self.flush()
                # 如果不再运行且队列为空且缓冲区为空，退出循环
                elif not self.running and self.data_queue.empty() and not self.buffer:
                    break
                continue
            except Exception as e:
                print(f"❌ [Writer Error] {e}")
                import traceback
                traceback.print_exc()

        # 最终刷新（确保所有数据都写入）
        if self.buffer:
            print("💾 [Writer] 最终刷新缓冲区...")
            self.flush()
        
        # 关闭所有连接
        for date_str, conn in self.connections.items():
            try:
                conn.close()
                print(f"✅ [Writer] 已关闭连接: {date_str}")
            except:
                pass
        
        print("✅ [Writer] 写入线程安全退出")

    def flush(self):
        """批量写入数据到数据库（重写版本，修复日期切换问题）"""
        if not self.buffer:
            return

        # 按日期分组数据（使用UTC时间，避免时区问题）
        data_by_date: Dict[str, Dict[str, List]] = {}
        
        # 统计信息（用于调试）
        date_range = {'min': None, 'max': None}
        
        for item in self.buffer:
            if not isinstance(item, dict) or 'data' not in item:
                continue
            
            data = item['data']
            if len(data) < 4:
                continue
            
            # 获取时间戳并确定日期（使用UTC时间，确保日期判断准确）
            exchange_time_us = data[-1]
            
            # 验证时间戳有效性
            if not isinstance(exchange_time_us, (int, float)) or exchange_time_us <= 0:
                print(f"⚠️ [Writer] 无效时间戳: {exchange_time_us} (类型: {type(exchange_time_us)}), 跳过此条数据")
                continue
            
            # 转换为datetime（使用UTC，避免时区问题）
            try:
                # 确保时间戳是数值类型
                timestamp_seconds = float(exchange_time_us) / 1_000_000
                
                # 使用UTC时区转换，确保日期准确
                dt = datetime.datetime.fromtimestamp(timestamp_seconds, tz=datetime.timezone.utc)
                date_str = dt.strftime("%Y-%m-%d")
                
                # 验证日期格式
                try:
                    datetime.datetime.strptime(date_str, "%Y-%m-%d")
                except ValueError:
                    print(f"⚠️ [Writer] 无效日期格式: {date_str}, 跳过此条数据")
                    continue
                
                # 更新日期范围（用于调试）
                if date_range['min'] is None or date_str < date_range['min']:
                    date_range['min'] = date_str
                if date_range['max'] is None or date_str > date_range['max']:
                    date_range['max'] = date_str
                    
            except (ValueError, OSError, OverflowError) as e:
                print(f"⚠️ [Writer] 时间戳转换失败: {exchange_time_us}, 错误: {e}, 跳过此条数据")
                continue
            
            if date_str not in data_by_date:
                data_by_date[date_str] = {'ticks': [], 'doms': []}
            
            if item['type'] == 'T':
                data_by_date[date_str]['ticks'].append(data)
            elif item['type'] == 'D':
                data_by_date[date_str]['doms'].append(data)

        # 记录缓冲区大小（用于调试）
        buffer_size = len(self.buffer)
        
        # 清空缓冲区（在写入之前清空，避免重复处理）
        self.buffer.clear()
        self.last_flush_time = time.time()

        # 如果没有有效数据，直接返回
        if not data_by_date:
            return

        # 按日期排序后再写入（确保按时间顺序处理）
        sorted_dates = sorted(data_by_date.keys())
        
        # 调试信息：如果日期范围跨度大，打印警告
        if date_range['min'] and date_range['max'] and date_range['min'] != date_range['max']:
            print(f"⚠️ [Writer] 缓冲区包含跨天数据: {date_range['min']} ~ {date_range['max']} (共 {len(sorted_dates)} 天)")
        
        # 按日期写入（统一处理，避免频繁切换）
        for date_str in sorted_dates:
            data_dict = data_by_date[date_str]
            
            # 验证：确保数据确实属于这个日期
            # 再次检查第一条和最后一条数据的日期，确保一致性
            tick_count = len(data_dict['ticks'])
            dom_count = len(data_dict['doms'])
            
            if tick_count == 0 and dom_count == 0:
                continue  # 跳过空日期组
            
            # 更新当前日期（用于内部状态跟踪）
            old_date = self.current_date
            self.current_date = date_str
            
            # 只在真正切换日期时才打印（减少重复日志）
            if date_str != self.last_logged_date:
                if self.last_logged_date:
                    print(f"📅 [Writer] 日期切换: {self.last_logged_date} → {date_str} (Ticks: {tick_count}, DOMs: {dom_count})")
                else:
                    print(f"📅 [Writer] 开始写入日期: {date_str} (Ticks: {tick_count}, DOMs: {dom_count})")
                self.last_logged_date = date_str
            
            conn = self.get_connection(date_str)
            
            try:
                # 优化：使用批量插入，提高写入性能
                # 先尝试直接插入（大多数情况下数据是新的），如果失败再检查重复
                conn.execute("BEGIN TRANSACTION")

                # 写入 Tick 数据（带去重：如果重复接收数据，不会产生重复记录）
                actual_tick_count = 0
                if data_dict['ticks']:
                    import pandas as pd
                    tick_cols = ['symbol', 'price', 'volume', 'side', 'exchange_time_us']
                    df_ticks = pd.DataFrame(data_dict['ticks'], columns=tick_cols)
                    
                    # 转换时间戳（ATAS发送的是UTC+0时间，直接使用UTC时间戳转换为naive datetime）
                    # 注意：DuckDB的TIMESTAMP类型不支持时区，所以使用UTC时间戳但转换为naive datetime
                    df_ticks['exchange_time'] = pd.to_datetime(df_ticks['exchange_time_us'] / 1_000_000, unit='s', utc=True).dt.tz_localize(None)
                    df_ticks['recv_time'] = pd.Timestamp.now(tz='UTC').tz_localize(None)
                    
                    # 验证：确保所有数据的日期都匹配目标日期
                    # 提取日期并检查
                    df_ticks['date_check'] = df_ticks['exchange_time'].dt.date.astype(str)
                    mismatched = df_ticks[df_ticks['date_check'] != date_str]
                    if not mismatched.empty:
                        print(f"⚠️ [Writer] 警告: 发现 {len(mismatched)} 条Tick数据的日期不匹配目标日期 {date_str}")
                        print(f"   不匹配的日期: {mismatched['date_check'].unique()}")
                        # 过滤掉不匹配的数据（防止写入错误的日期文件）
                        df_ticks = df_ticks[df_ticks['date_check'] == date_str]
                    
                    if df_ticks.empty:
                        print(f"⚠️ [Writer] 日期 {date_str} 的Tick数据全部被过滤（日期不匹配）")
                    else:
                        # 只选择需要的列
                        df_ticks_insert = df_ticks[['symbol', 'price', 'volume', 'side', 'exchange_time', 'recv_time']]
                        # 使用register方法批量插入，并去重（基于 symbol + exchange_time）
                        conn.register('temp_ticks', df_ticks_insert)
                        # 使用 WHERE NOT EXISTS 去重：如果 (symbol, exchange_time) 已存在，则跳过
                        conn.execute("""
                            INSERT INTO ticks 
                            SELECT * FROM temp_ticks
                            WHERE NOT EXISTS (
                                SELECT 1 FROM ticks t 
                                WHERE t.symbol = temp_ticks.symbol 
                                AND t.exchange_time = temp_ticks.exchange_time
                            )
                        """)
                        conn.unregister('temp_ticks')
                        actual_tick_count = len(df_ticks_insert)

                # 写入 DOM 数据（带去重：如果重复接收数据，不会产生重复记录）
                actual_dom_count = 0
                if data_dict['doms']:
                    import pandas as pd
                    dom_cols = ['symbol', 'bids', 'asks', 'exchange_time_us']
                    df_doms = pd.DataFrame(data_dict['doms'], columns=dom_cols)
                    
                    # 转换时间戳（ATAS发送的是UTC+0时间，直接使用UTC时间戳转换为naive datetime）
                    # 注意：DuckDB的TIMESTAMP类型不支持时区，所以使用UTC时间戳但转换为naive datetime
                    df_doms['exchange_time'] = pd.to_datetime(df_doms['exchange_time_us'] / 1_000_000, unit='s', utc=True).dt.tz_localize(None)
                    df_doms['recv_time'] = pd.Timestamp.now(tz='UTC').tz_localize(None)
                    
                    # 验证：确保所有数据的日期都匹配目标日期
                    df_doms['date_check'] = df_doms['exchange_time'].dt.date.astype(str)
                    mismatched = df_doms[df_doms['date_check'] != date_str]
                    if not mismatched.empty:
                        print(f"⚠️ [Writer] 警告: 发现 {len(mismatched)} 条DOM数据的日期不匹配目标日期 {date_str}")
                        print(f"   不匹配的日期: {mismatched['date_check'].unique()}")
                        # 过滤掉不匹配的数据（防止写入错误的日期文件）
                        df_doms = df_doms[df_doms['date_check'] == date_str]
                    
                    if df_doms.empty:
                        print(f"⚠️ [Writer] 日期 {date_str} 的DOM数据全部被过滤（日期不匹配）")
                    else:
                        # 只选择需要的列
                        df_doms_insert = df_doms[['symbol', 'bids', 'asks', 'exchange_time', 'recv_time']]
                        # 使用register方法批量插入，并去重（基于 symbol + exchange_time）
                        conn.register('temp_doms', df_doms_insert)
                        # 使用 WHERE NOT EXISTS 去重：如果 (symbol, exchange_time) 已存在，则跳过
                        conn.execute("""
                            INSERT INTO depth 
                            SELECT * FROM temp_doms
                            WHERE NOT EXISTS (
                                SELECT 1 FROM depth d 
                                WHERE d.symbol = temp_doms.symbol 
                                AND d.exchange_time = temp_doms.exchange_time
                            )
                        """)
                        conn.unregister('temp_doms')
                        actual_dom_count = len(df_doms_insert)

                conn.execute("COMMIT")
                
                # 计算实际写入的数据量（使用实际写入的数量）
                count = actual_tick_count + actual_dom_count
                
                if count > 0:
                    self.total_written += count
                    if self.status_callback:
                        self.status_callback(f"Total: {self.total_written:,}", "blue")

            except Exception as e:
                try:
                    conn.execute("ROLLBACK")
                    print(f"❌ [DB TRANSACTION] Rolled back batch due to error: {e}")
                except:
                    print(f"❌ [DB FATAL] Could not rollback (Original Error: {e}). Data lost.")
                import traceback
                traceback.print_exc()


# =======================================================================
# 网络接收线程 (生产者)
# =======================================================================
class ReceiverThread(threading.Thread):
    def __init__(self, data_queue: queue.Queue, status_callback=None):
        super().__init__(daemon=True)
        self.data_queue = data_queue
        self.status_callback = status_callback
        self.running = True
        self.sock: Optional[socket.socket] = None
        self.db_initialized = False
        self.total_received = 0
        self.last_data_time = time.time()  # 记录最后接收数据的时间

    def run(self):
        """主循环：从UDP接收数据并放入队列"""
        print("🚀 [Receiver] 接收线程启动")
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32 * 1024 * 1024)
        self.sock.bind((UDP_IP, UDP_PORT))
        self.sock.settimeout(SOCKET_TIMEOUT)

        if self.status_callback:
            self.status_callback(f"Listening on {UDP_PORT}...", "blue")

        while self.running:
            try:
                data, _ = self.sock.recvfrom(65535)
                text = data.decode('utf-8')
                messages = text.strip().split('\n')

                for msg in messages:
                    if not msg:
                        continue
                    
                    parts = msg.split(',')
                    msg_type = parts[0]

                    # 初始化数据库（使用第一条消息的时间戳）
                    if not self.db_initialized and (msg_type == 'T' or msg_type == 'D'):
                        try:
                            ts = parts[5] if msg_type == 'T' else parts[4]
                            # 使用阻塞put，确保不丢失数据
                            self.data_queue.put({'init': ts}, block=True)
                            self.db_initialized = True
                        except:
                            continue

                    if not self.db_initialized:
                        continue

                    # 解析 Tick 数据
                    if msg_type == 'T' and len(parts) >= 6:
                        exchange_time_us = ticks_to_datetime_us(parts[5])
                        row = (parts[1], float(parts[2]), float(parts[3]), parts[4], exchange_time_us)
                        # 使用阻塞put，确保不丢失数据（会等待直到队列有空间）
                        self.data_queue.put({'type': 'T', 'data': row}, block=True)
                        self.total_received += 1
                        self.last_data_time = time.time()  # 更新最后接收时间

                    # 解析 DOM 数据
                    elif msg_type == 'D' and len(parts) >= 5:
                        exchange_time_us = ticks_to_datetime_us(parts[4])
                        row = (parts[1], parts[2], parts[3], exchange_time_us)
                        # 使用阻塞put，确保不丢失数据（会等待直到队列有空间）
                        self.data_queue.put({'type': 'D', 'data': row}, block=True)
                        self.total_received += 1
                        self.last_data_time = time.time()  # 更新最后接收时间

            except socket.timeout:
                # 检查是否超时无数据（自动停止）
                if self.db_initialized and (time.time() - self.last_data_time) > AUTO_STOP_TIMEOUT:
                    print(f"\n⏰ [Auto-Stop] 已 {AUTO_STOP_TIMEOUT} 秒未接收到数据，自动停止...")
                    self.running = False
                    break
                continue
            except Exception as e:
                print(f"❌ [Receiver Error] {e}")
                import traceback
                traceback.print_exc()

        if self.sock:
            self.sock.close()
        print("✅ [Receiver] 接收线程退出")


# =======================================================================
# 数据读取工具（用于回溯）
# =======================================================================
class DataReader:
    """数据读取工具，支持跨天连续读取"""
    
    def __init__(self, db_folder: str = DB_FOLDER):
        self.db_folder = db_folder
    
    def list_available_dates(self) -> List[str]:
        """列出所有可用的日期"""
        dates = []
        if not os.path.exists(self.db_folder):
            return dates
        
        for filename in os.listdir(self.db_folder):
            if filename.startswith("market_data_") and filename.endswith(".duckdb"):
                date_str = filename.replace("market_data_", "").replace(".duckdb", "")
                try:
                    # 验证日期格式
                    datetime.datetime.strptime(date_str, "%Y-%m-%d")
                    dates.append(date_str)
                except:
                    continue
        
        return sorted(dates)
    
    def load_data_range(self, start_date: str, end_date: str, symbol: str = "ES") -> tuple:
        """
        加载指定日期范围的数据（跨天连续读取）
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            symbol: 合约代码，默认 "ES"
        
        Returns:
            (df_ticks, df_depth): Tick数据和DOM数据的DataFrame
        """
        available_dates = self.list_available_dates()
        
        # 筛选日期范围
        target_dates = [d for d in available_dates if start_date <= d <= end_date]
        
        if not target_dates:
            print(f"⚠️ 警告: 在 {start_date} 到 {end_date} 范围内没有找到数据")
            return None, None
        
        print(f"📅 加载日期范围: {target_dates[0]} 到 {target_dates[-1]} (共 {len(target_dates)} 天)")
        
        all_ticks = []
        all_depth = []
        
        # 逐个日期加载数据
        for date_str in target_dates:
            db_file = get_db_file_path(date_str)
            if not os.path.exists(db_file):
                print(f"⚠️ 警告: 文件不存在: {db_file}")
                continue
            
            try:
                conn = duckdb.connect(db_file)
                
                # 加载 Tick 数据
                tick_query = f"""
                    SELECT 
                        exchange_time, price, volume, side, recv_time 
                    FROM ticks 
                    WHERE symbol = '{symbol}'
                    ORDER BY exchange_time ASC;
                """
                df_ticks = conn.execute(tick_query).df()
                if not df_ticks.empty:
                    all_ticks.append(df_ticks)
                
                # 加载 DOM 数据
                depth_query = f"""
                    SELECT 
                        exchange_time, bids, asks, recv_time 
                    FROM depth 
                    WHERE symbol = '{symbol}'
                    ORDER BY exchange_time ASC;
                """
                df_depth = conn.execute(depth_query).df()
                if not df_depth.empty:
                    all_depth.append(df_depth)
                
                conn.close()
                print(f"✅ 已加载 {date_str}: {len(df_ticks)} ticks, {len(df_depth)} DOM")
                
            except Exception as e:
                print(f"❌ 加载 {date_str} 时出错: {e}")
                continue
        
        # 合并所有日期的数据
        if all_ticks:
            import pandas as pd
            df_ticks_combined = pd.concat(all_ticks, ignore_index=True)
            df_ticks_combined = df_ticks_combined.set_index('exchange_time')
            df_ticks_combined = df_ticks_combined.sort_index()
        else:
            df_ticks_combined = None
        
        if all_depth:
            import pandas as pd
            df_depth_combined = pd.concat(all_depth, ignore_index=True)
            df_depth_combined = df_depth_combined.set_index('exchange_time')
            df_depth_combined = df_depth_combined.sort_index()
        else:
            df_depth_combined = None
        
        return df_ticks_combined, df_depth_combined


# =======================================================================
# 主程序（命令行界面）
# =======================================================================
class StorageModule:
    """数据存储模块主类"""
    
    def __init__(self):
        self.data_queue = queue.Queue(maxsize=QUEUE_SIZE)
        self.receiver: Optional[ReceiverThread] = None
        self.writer: Optional[DbWriterThread] = None
        self.running = False
        self.shutdown_requested = False  # 标记是否请求关闭
    
    def start(self):
        """启动数据接收和存储"""
        if self.running:
            print("⚠️ 模块已在运行")
            return
        
        # 首次运行：自动创建数据库文件夹
        db_folder_path = ensure_db_folder()
        if not os.path.exists(db_folder_path):
            print(f"📁 首次运行：自动创建数据库文件夹: {os.path.abspath(db_folder_path)}")
        else:
            print(f"📁 数据库文件夹: {os.path.abspath(db_folder_path)}")
        
        self.running = True
        print("🚀 启动数据存储模块...")
        
        def status_callback(msg, color="blue"):
            print(f"[STATUS] {msg}")
        
        self.writer = DbWriterThread(self.data_queue, status_callback)
        self.receiver = ReceiverThread(self.data_queue, status_callback)
        
        self.writer.start()
        self.receiver.start()
        
        print("✅ 数据存储模块已启动")
        print("💡 按 Ctrl+C 停止（或等待30秒无数据自动停止）")
    
    def stop(self, graceful=True):
        """
        停止数据接收和存储
        
        Args:
            graceful: 是否优雅关闭（等待数据写入完成）
        """
        if not self.running:
            return
        
        if self.shutdown_requested:
            return  # 已经在关闭中
        
        self.shutdown_requested = True
        print("\n🛑 正在停止数据存储模块...")
        
        if graceful:
            print("📝 等待数据写入完成...")
        
        self.running = False
        
        if self.receiver:
            self.receiver.running = False
        
        # 等待接收线程结束
        if self.receiver and self.receiver.is_alive():
            self.receiver.join(timeout=5.0)
            if self.receiver.is_alive():
                print("⚠️ 接收线程未在5秒内结束")
        
        # 等待写入线程完成所有数据写入
        if graceful and self.writer:
            print("💾 正在写入剩余数据...")
            
            # 标记写入线程停止（但让它继续处理队列中的数据）
            self.writer.running = False
            
            # 等待队列清空和缓冲区写入
            max_wait_time = 120.0  # 增加等待时间到120秒（适应大数据量）
            start_wait = time.time()
            last_q_size = self.data_queue.qsize()
            last_written = self.writer.total_written
            
            while (not self.data_queue.empty() or self.writer.buffer) and (time.time() - start_wait) < max_wait_time:
                q_size = self.data_queue.qsize()
                written = self.writer.total_written
                
                # 如果队列大小或写入数量有变化，说明还在处理
                if q_size != last_q_size or written != last_written:
                    if q_size > 0:
                        print(f"   队列剩余: {q_size:,} 条，缓冲区: {len(self.writer.buffer):,} 条，等待写入...")
                    elif self.writer.buffer:
                        print(f"   缓冲区剩余: {len(self.writer.buffer):,} 条，等待写入...")
                    last_q_size = q_size
                    last_written = written
                
                time.sleep(0.2)  # 更频繁地检查，加快响应
            
            # 等待写入线程结束（给足够时间完成写入）
            if self.writer.is_alive():
                remaining_time = max_wait_time - (time.time() - start_wait)
                if remaining_time > 0:
                    self.writer.join(timeout=min(remaining_time, 30.0))
                    if self.writer.is_alive():
                        print("⚠️ 写入线程未在指定时间内结束，但数据可能已写入")
                else:
                    print("⚠️ 等待时间已用完，但写入线程仍在运行")
        
        print("✅ 数据存储模块已停止")
        if self.writer:
            print(f"📊 总共写入: {self.writer.total_written:,} 条数据")
    
    def run(self):
        """运行主循环"""
        try:
            self.start()
            last_stats_time = time.time()
            
            while self.running:
                time.sleep(0.5)  # 更频繁的检查，适应高速回放
                
                # 定期打印统计信息（每5秒一次）
                now = time.time()
                if self.receiver and self.writer and (now - last_stats_time) >= 5.0:
                    q_size = self.data_queue.qsize()
                    received = self.receiver.total_received
                    written = self.writer.total_written
                    
                    # 计算速率
                    elapsed = now - last_stats_time
                    receive_rate = (received - getattr(self, '_last_received', 0)) / elapsed if elapsed > 0 else 0
                    write_rate = (written - getattr(self, '_last_written', 0)) / elapsed if elapsed > 0 else 0
                    
                    self._last_received = received
                    self._last_written = written
                    last_stats_time = now
                    
                    # 队列状态颜色提示
                    if q_size > QUEUE_SIZE * 0.8:
                        queue_status = f"⚠️ 队列接近满载: {q_size:,}/{QUEUE_SIZE:,}"
                    elif q_size > QUEUE_SIZE * 0.5:
                        queue_status = f"⚡ 队列: {q_size:,}"
                    else:
                        queue_status = f"✅ 队列: {q_size:,}"
                    
                    print(f"[STATS] {queue_status} | "
                          f"接收: {received:,} ({receive_rate:.0f}/s) | "
                          f"写入: {written:,} ({write_rate:.0f}/s)")
                
                # 检查接收线程是否因为自动停止而退出
                if self.receiver and not self.receiver.is_alive() and self.receiver.running == False:
                    print("\n⏰ 检测到数据流已停止，准备关闭...")
                    break
                    
        except KeyboardInterrupt:
            print("\n\n⚠️ 收到 Ctrl+C 信号，正在优雅关闭...")
        finally:
            # 优雅关闭：确保所有数据都写入
            self.stop(graceful=True)


if __name__ == "__main__":
    module = StorageModule()
    module.run()

