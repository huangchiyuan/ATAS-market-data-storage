"""
数据读取工具（用于回溯）
======================

功能：
1. 从数据库文件夹中读取指定日期范围的数据
2. 支持跨天连续读取
3. 自动合并多天的数据
4. 提供便捷的接口供回测使用

使用示例：
    from data_reader_for_backtest import DataReaderForBacktest
    
    reader = DataReaderForBacktest()
    
    # 读取单天数据
    df_ticks, df_depth = reader.load_single_day("2025-01-15", "ES")
    
    # 读取多天数据
    df_ticks, df_depth = reader.load_date_range("2025-01-15", "2025-01-20", "ES")
    
    # 列出所有可用日期
    dates = reader.list_available_dates()
"""

import os
import datetime
from typing import Optional, List, Tuple
import duckdb
import pandas as pd
from pathlib import Path

from data_storage_module import DB_FOLDER, get_db_file_path


class DataReaderForBacktest:
    """数据读取工具，用于回溯测试"""
    
    def __init__(self, db_folder: str = DB_FOLDER):
        """
        初始化数据读取器
        
        Args:
            db_folder: 数据库文件夹路径，默认使用 data_storage_module 中的 DB_FOLDER
        """
        self.db_folder = db_folder
        if not os.path.exists(self.db_folder):
            print(f"⚠️ 警告: 数据库文件夹不存在: {self.db_folder}")
            Path(self.db_folder).mkdir(parents=True, exist_ok=True)
    
    def list_available_dates(self) -> List[str]:
        """
        列出所有可用的日期
        
        Returns:
            日期列表，格式为 ["YYYY-MM-DD", ...]，已排序
        """
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
                except ValueError:
                    continue
        
        return sorted(dates)
    
    def get_date_info(self, date_str: str) -> Optional[dict]:
        """
        获取指定日期的数据统计信息
        
        Args:
            date_str: 日期字符串 (YYYY-MM-DD)
        
        Returns:
            包含统计信息的字典，如果文件不存在返回 None
        """
        db_file = get_db_file_path(date_str)
        if not os.path.exists(db_file):
            return None
        
        try:
            conn = duckdb.connect(db_file)
            
            # 获取 Tick 数据统计
            tick_stats = conn.execute("""
                SELECT 
                    COUNT(*) as count,
                    MIN(exchange_time) as min_time,
                    MAX(exchange_time) as max_time,
                    COUNT(DISTINCT symbol) as symbols
                FROM ticks
            """).fetchone()
            
            # 获取 DOM 数据统计
            depth_stats = conn.execute("""
                SELECT 
                    COUNT(*) as count,
                    MIN(exchange_time) as min_time,
                    MAX(exchange_time) as max_time,
                    COUNT(DISTINCT symbol) as symbols
                FROM depth
            """).fetchone()
            
            conn.close()
            
            return {
                'date': date_str,
                'ticks': {
                    'count': tick_stats[0] if tick_stats else 0,
                    'min_time': tick_stats[1] if tick_stats else None,
                    'max_time': tick_stats[2] if tick_stats else None,
                    'symbols': tick_stats[3] if tick_stats else 0,
                },
                'depth': {
                    'count': depth_stats[0] if depth_stats else 0,
                    'min_time': depth_stats[1] if depth_stats else None,
                    'max_time': depth_stats[2] if depth_stats else None,
                    'symbols': depth_stats[3] if depth_stats else 0,
                }
            }
        except Exception as e:
            print(f"❌ 读取 {date_str} 统计信息时出错: {e}")
            return None
    
    def load_single_day(self, date_str: str, symbol: str = "ES") -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """
        加载单天的数据
        
        Args:
            date_str: 日期字符串 (YYYY-MM-DD)
            symbol: 合约代码，默认 "ES"
        
        Returns:
            (df_ticks, df_depth): Tick数据和DOM数据的DataFrame
        """
        return self.load_date_range(date_str, date_str, symbol)
    
    def load_date_range(
        self, 
        start_date: str, 
        end_date: str, 
        symbol: str = "ES"
    ) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """
        加载指定日期范围的数据（跨天连续读取）
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            symbol: 合约代码，默认 "ES"
        
        Returns:
            (df_ticks, df_depth): Tick数据和DOM数据的DataFrame
                - df_ticks: 索引为 exchange_time，列包括 price, volume, side, recv_time
                - df_depth: 索引为 exchange_time，列包括 bids, asks, recv_time
        """
        available_dates = self.list_available_dates()
        
        # 筛选日期范围
        target_dates = [d for d in available_dates if start_date <= d <= end_date]
        
        if not target_dates:
            print(f"⚠️ 警告: 在 {start_date} 到 {end_date} 范围内没有找到数据")
            print(f"   可用日期: {available_dates[:5]}..." if len(available_dates) > 5 else f"   可用日期: {available_dates}")
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
                import traceback
                traceback.print_exc()
                continue
        
        # 合并所有日期的数据
        if all_ticks:
            df_ticks_combined = pd.concat(all_ticks, ignore_index=True)
            df_ticks_combined = df_ticks_combined.set_index('exchange_time')
            df_ticks_combined = df_ticks_combined.sort_index()
            print(f"✅ 合并后 Tick 数据: {len(df_ticks_combined)} 条")
        else:
            df_ticks_combined = None
            print("⚠️ 没有 Tick 数据")
        
        if all_depth:
            df_depth_combined = pd.concat(all_depth, ignore_index=True)
            df_depth_combined = df_depth_combined.set_index('exchange_time')
            df_depth_combined = df_depth_combined.sort_index()
            print(f"✅ 合并后 DOM 数据: {len(df_depth_combined)} 条")
        else:
            df_depth_combined = None
            print("⚠️ 没有 DOM 数据")
        
        return df_ticks_combined, df_depth_combined
    
    def load_recent_days(self, days: int, symbol: str = "ES") -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """
        加载最近N天的数据
        
        Args:
            days: 天数
            symbol: 合约代码，默认 "ES"
        
        Returns:
            (df_ticks, df_depth): Tick数据和DOM数据的DataFrame
        """
        available_dates = self.list_available_dates()
        if not available_dates:
            print("⚠️ 没有可用的数据")
            return None, None
        
        if len(available_dates) <= days:
            start_date = available_dates[0]
        else:
            start_date = available_dates[-days]
        
        end_date = available_dates[-1]
        
        print(f"📅 加载最近 {days} 天数据: {start_date} 到 {end_date}")
        return self.load_date_range(start_date, end_date, symbol)
    
    def print_summary(self):
        """打印所有可用数据的摘要信息"""
        dates = self.list_available_dates()
        if not dates:
            print("📊 没有可用的数据")
            return
        
        print(f"\n📊 数据摘要 (共 {len(dates)} 天)")
        print("=" * 80)
        print(f"{'日期':<12} {'Tick数量':<12} {'DOM数量':<12} {'时间范围':<40}")
        print("-" * 80)
        
        for date_str in dates:
            info = self.get_date_info(date_str)
            if info:
                tick_count = info['ticks']['count']
                depth_count = info['depth']['count']
                min_time = info['ticks']['min_time'] or info['depth']['min_time']
                max_time = info['ticks']['max_time'] or info['depth']['max_time']
                
                time_range = ""
                if min_time and max_time:
                    time_range = f"{min_time} ~ {max_time}"
                
                print(f"{date_str:<12} {tick_count:<12,} {depth_count:<12,} {time_range:<40}")
        
        print("=" * 80)


# =======================================================================
# 使用示例
# =======================================================================
if __name__ == "__main__":
    # 创建读取器
    reader = DataReaderForBacktest()
    
    # 打印摘要
    reader.print_summary()
    
    # 列出所有可用日期
    dates = reader.list_available_dates()
    print(f"\n📅 可用日期: {dates}")
    
    if dates:
        # 加载第一天的数据
        first_date = dates[0]
        print(f"\n📖 加载单天数据: {first_date}")
        df_ticks, df_depth = reader.load_single_day(first_date, "ES")
        
        if df_ticks is not None:
            print(f"\nTick 数据预览:")
            print(df_ticks.head())
        
        if df_depth is not None:
            print(f"\nDOM 数据预览:")
            print(df_depth.head())
        
        # 如果有多天数据，加载日期范围
        if len(dates) > 1:
            print(f"\n📖 加载日期范围: {dates[0]} 到 {dates[-1]}")
            df_ticks_range, df_depth_range = reader.load_date_range(dates[0], dates[-1], "ES")
            
            if df_ticks_range is not None:
                print(f"\n合并后 Tick 数据: {len(df_ticks_range)} 条")
                print(f"时间范围: {df_ticks_range.index.min()} ~ {df_ticks_range.index.max()}")

