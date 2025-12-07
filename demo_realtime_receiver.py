"""
实时数据接收 Demo
================

这是一个简单的示例程序，演示如何接收和显示从 ATAS 平台发送的实时市场数据。

功能：
1. 从 UDP 端口接收 Tick 和 DOM 数据
2. 实时解析和显示数据
3. 统计接收速率

使用方法：
    python demo_realtime_receiver.py
"""

import socket
import time
from datetime import datetime
from typing import Optional

# --- 配置 ---
UDP_IP = "127.0.0.1"
UDP_PORT = 5555
SOCKET_TIMEOUT = 2.0

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


def ticks_to_datetime(ticks_str: str) -> datetime:
    """将 C# ticks 转换为 datetime 对象"""
    try:
        microseconds = ticks_to_datetime_us(ticks_str)
        return datetime.fromtimestamp(microseconds / 1_000_000)
    except:
        return datetime.now()


def parse_dom_string(dom_str: str) -> tuple:
    """
    解析 DOM 字符串
    
    格式: "price@volume|price@volume|..."
    返回: [(price, volume), ...]
    """
    if not dom_str or dom_str == "0@0":
        return []
    
    levels = []
    for level_str in dom_str.split('|'):
        if '@' in level_str:
            try:
                price_str, volume_str = level_str.split('@')
                price = float(price_str)
                volume = float(volume_str)
                if price > 0 and volume > 0:
                    levels.append((price, volume))
            except:
                continue
    return levels


def format_dom_levels(levels: list, max_levels: int = 5) -> str:
    """格式化 DOM 档位显示"""
    if not levels:
        return "空"
    
    display_levels = levels[:max_levels]
    formatted = []
    for price, volume in display_levels:
        formatted.append(f"{price:.2f}@{volume:.0f}")
    
    if len(levels) > max_levels:
        formatted.append(f"...(+{len(levels) - max_levels})")
    
    return " | ".join(formatted)


class RealtimeReceiver:
    """实时数据接收器"""
    
    def __init__(self):
        self.sock: Optional[socket.socket] = None
        self.running = False
        self.total_received = 0
        self.tick_count = 0
        self.dom_count = 0
        self.heartbeat_count = 0
        self.start_time = None
        self.last_stats_time = None
        
        # 最新价格缓存
        self.latest_prices = {}
        self.latest_dom = {}
    
    def start(self):
        """启动接收器"""
        print("=" * 80)
        print("🚀 实时数据接收 Demo")
        print("=" * 80)
        print(f"📡 监听地址: {UDP_IP}:{UDP_PORT}")
        print("💡 请确保 ATAS 平台已启动并加载了 NFQE_Bridge_UDP 指标")
        print("💡 按 Ctrl+C 停止")
        print("=" * 80)
        print()
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)  # 4MB 缓冲区
        self.sock.bind((UDP_IP, UDP_PORT))
        self.sock.settimeout(SOCKET_TIMEOUT)
        
        self.running = True
        self.start_time = time.time()
        self.last_stats_time = time.time()
    
    def stop(self):
        """停止接收器"""
        self.running = False
        if self.sock:
            self.sock.close()
        print("\n" + "=" * 80)
        print("✅ 接收器已停止")
        print("=" * 80)
    
    def print_stats(self):
        """打印统计信息"""
        now = time.time()
        elapsed = now - self.start_time if self.start_time else 1.0
        
        if elapsed > 0:
            rate = self.total_received / elapsed
            print(f"\n📊 统计信息:")
            print(f"   总接收: {self.total_received:,} 条")
            print(f"   Tick: {self.tick_count:,} 条")
            print(f"   DOM: {self.dom_count:,} 条")
            print(f"   心跳: {self.heartbeat_count:,} 条")
            print(f"   平均速率: {rate:.0f} 条/秒")
            print(f"   运行时间: {elapsed:.1f} 秒")
            print()
    
    def process_tick(self, parts: list):
        """处理 Tick 数据"""
        if len(parts) < 6:
            return
        
        symbol = parts[1]
        price = float(parts[2])
        volume = float(parts[3])
        side = parts[4]
        exchange_time_ticks = parts[5]
        
        # 转换时间
        exchange_time = ticks_to_datetime(exchange_time_ticks)
        
        # 更新最新价格
        self.latest_prices[symbol] = {
            'price': price,
            'volume': volume,
            'side': side,
            'time': exchange_time
        }
        
        # 显示
        side_emoji = "🟢" if side == "BUY" else "🔴" if side == "SELL" else "⚪"
        print(f"[TICK] {side_emoji} {symbol:6s} | 价格: {price:10.2f} | 数量: {volume:8.0f} | "
              f"方向: {side:4s} | 时间: {exchange_time.strftime('%H:%M:%S.%f')[:-3]}")
        
        self.tick_count += 1
        self.total_received += 1
    
    def process_dom(self, parts: list):
        """处理 DOM 数据"""
        if len(parts) < 5:
            return
        
        symbol = parts[1]
        bids_str = parts[2]
        asks_str = parts[3]
        exchange_time_ticks = parts[4]
        
        # 解析 DOM
        bids = parse_dom_string(bids_str)
        asks = parse_dom_string(asks_str)
        
        # 转换时间
        exchange_time = ticks_to_datetime(exchange_time_ticks)
        
        # 计算最佳买卖价
        best_bid = bids[0][0] if bids else 0.0
        best_ask = asks[0][0] if asks else 0.0
        spread = best_ask - best_bid if best_bid > 0 and best_ask > 0 else 0.0
        
        # 更新最新 DOM
        self.latest_dom[symbol] = {
            'bids': bids,
            'asks': asks,
            'best_bid': best_bid,
            'best_ask': best_ask,
            'spread': spread,
            'time': exchange_time
        }
        
        # 显示
        print(f"[DOM]  {symbol:6s} | Bid: {best_bid:10.2f} | Ask: {best_ask:10.2f} | "
              f"Spread: {spread:6.2f} | 时间: {exchange_time.strftime('%H:%M:%S.%f')[:-3]}")
        
        # 显示前几档（可选）
        if bids or asks:
            if bids:
                print(f"        Bids: {format_dom_levels(bids, max_levels=3)}")
            if asks:
                print(f"        Asks: {format_dom_levels(asks, max_levels=3)}")
        
        self.dom_count += 1
        self.total_received += 1
    
    def process_heartbeat(self, parts: list):
        """处理心跳数据"""
        if len(parts) < 3:
            return
        
        symbol = parts[1]
        local_time_ticks = parts[2]
        local_time = ticks_to_datetime(local_time_ticks)
        
        # 心跳通常不显示，只统计
        self.heartbeat_count += 1
        self.total_received += 1
    
    def run(self):
        """运行主循环"""
        try:
            self.start()
            
            while self.running:
                try:
                    data, addr = self.sock.recvfrom(65535)
                    text = data.decode('utf-8')
                    messages = text.strip().split('\n')
                    
                    for msg in messages:
                        if not msg:
                            continue
                        
                        parts = msg.split(',')
                        if len(parts) < 2:
                            continue
                        
                        msg_type = parts[0]
                        
                        # 处理不同类型的消息
                        if msg_type == 'T':
                            self.process_tick(parts)
                        elif msg_type == 'D':
                            self.process_dom(parts)
                        elif msg_type == 'H':
                            self.process_heartbeat(parts)
                    
                    # 每 10 秒打印一次统计信息
                    now = time.time()
                    if now - self.last_stats_time >= 10.0:
                        self.print_stats()
                        self.last_stats_time = now
                
                except socket.timeout:
                    # 超时，继续等待
                    continue
                except KeyboardInterrupt:
                    print("\n\n⚠️ 收到 Ctrl+C 信号，正在停止...")
                    break
                except Exception as e:
                    print(f"❌ 错误: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
        
        finally:
            self.stop()
            self.print_stats()
            
            # 显示最新状态
            if self.latest_prices or self.latest_dom:
                print("\n📊 最新数据快照:")
                print("-" * 80)
                
                if self.latest_prices:
                    print("最新价格:")
                    for symbol, data in self.latest_prices.items():
                        print(f"  {symbol}: {data['price']:.2f} ({data['side']}) @ {data['time'].strftime('%H:%M:%S')}")
                
                if self.latest_dom:
                    print("\n最新 DOM:")
                    for symbol, data in self.latest_dom.items():
                        print(f"  {symbol}: Bid={data['best_bid']:.2f} Ask={data['best_ask']:.2f} "
                              f"Spread={data['spread']:.2f} @ {data['time'].strftime('%H:%M:%S')}")


if __name__ == "__main__":
    receiver = RealtimeReceiver()
    receiver.run()

