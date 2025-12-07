using System;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using System.Timers;
using ATAS.Indicators;
using ATAS.Indicators.Technical;

// 解决 Timer 命名冲突
using Timer = System.Timers.Timer;
using Utils.Common.Logging;

namespace ATAS.Indicators.Technical
{
    [DisplayName("NFQE DataPump V4.0 (Live Trading Optimized)")]
    [Description("专为实盘优化的数据桥：Tick即时发送，DOM智能缓冲，带高精度时间戳。")]
    [Category("Norden Flow")]
    public class NFQE_Bridge_UDP : Indicator
    {
        #region 参数设置
        [Display(Name = "Python IP", GroupName = "Connection", Order = 10)]
        public string PythonIP { get; set; } = "127.0.0.1";

        [Display(Name = "Python Port", GroupName = "Connection", Order = 20)]
        public int PythonPort { get; set; } = 5555;

        [Display(Name = "DOM Depth", GroupName = "Data", Order = 30)]
        public int DepthLevels { get; set; } = 15;

        // 缓冲区阈值 (仅用于 DOM 数据)
        private const int BATCH_THRESHOLD = 8192;
        #endregion

        private UdpClient _udpClient;
        private IPEndPoint _endPoint;
        private Timer _flushTimer;

        private string _lastDepthHash = "";

        // 批量发送缓冲区 (主要用于 DOM 和 心跳)
        private readonly StringBuilder _batchBuffer = new StringBuilder(BATCH_THRESHOLD * 2);
        private readonly object _lockObj = new object();

        protected override void OnInitialize()
        {
            try
            {
                InitializeNetwork();

                // [实盘优化] 将刷新间隔从 500ms 降低到 50ms
                // 这意味着 DOM 数据最慢也会在 50ms 内发出，满足人眼和策略需求
                _flushTimer = new Timer(50);
                _flushTimer.Elapsed += OnTimerTick;
                _flushTimer.AutoReset = true;
                _flushTimer.Enabled = true;
            }
            catch (Exception ex)
            {
                this.LogInfo($"NFQE Init Error: {ex.Message}");
            }
        }

        private void InitializeNetwork()
        {
            if (_udpClient != null) _udpClient.Close();
            _udpClient = new UdpClient();

            // 增大 OS 发送缓冲区，防止在 500x 回放或高频快市时丢包
            _udpClient.Client.SendBufferSize = 1024 * 1024; // 1MB Buffer

            _endPoint = new IPEndPoint(IPAddress.Parse(PythonIP), PythonPort);
        }

        private void OnTimerTick(object sender, ElapsedEventArgs e)
        {
            // 1. 发送心跳 (H,Symbol,LocalTime) - 用于检测连接状态
            if (this.InstrumentInfo != null)
            {
                AddToBatch($"H,{this.InstrumentInfo.Instrument},{DateTime.UtcNow.Ticks}");
            }

            // 2. 强制刷新缓冲区 (把积压的 DOM 发出去)
            FlushBatch();
        }

        // 将数据添加到缓冲区 (用于高吞吐量的 DOM)
        private void AddToBatch(string line)
        {
            lock (_lockObj)
            {
                _batchBuffer.Append(line);
                _batchBuffer.Append('\n');

                if (_batchBuffer.Length >= BATCH_THRESHOLD)
                {
                    FlushBatch();
                }
            }
        }

        // 执行网络发送
        private void FlushBatch()
        {
            lock (_lockObj)
            {
                if (_batchBuffer.Length == 0) return;

                if (_udpClient != null)
                {
                    try
                    {
                        string payload = _batchBuffer.ToString();
                        byte[] bytes = Encoding.UTF8.GetBytes(payload);
                        _udpClient.Send(bytes, bytes.Length, _endPoint);
                    }
                    catch { /* 忽略网络错误，保持运行 */ }
                }

                _batchBuffer.Clear();
            }
        }

        // [实盘核心] 直接发送，绕过缓冲区
        private void SendRaw(string message)
        {
            if (_udpClient != null)
            {
                try
                {
                    byte[] bytes = Encoding.UTF8.GetBytes(message);
                    _udpClient.Send(bytes, bytes.Length, _endPoint);
                }
                catch { }
            }
        }

        protected override void OnNewTrade(MarketDataArg arg)
        {
            try
            {
                string side = arg.Direction == TradeDirection.Buy ? "BUY" : (arg.Direction == TradeDirection.Sell ? "SELL" : "NONE");

                // 格式: T,Symbol,Price,Volume,Side,ExchangeTimeTicks
                string msg = $"T,{this.InstrumentInfo.Instrument},{arg.Price},{arg.Volume},{side},{arg.Time.Ticks}";

                // [实盘优化] Tick 数据极其重要且体积小，直接立即发送！
                // 这样可以实现 <1ms 的传输延迟
                SendRaw(msg + "\n");
            }
            catch { }
        }

        protected override void OnBestBidAskChanged(MarketDataArg arg)
        {
            try
            {
                var provider = this.MarketDepthInfo;
                if (provider == null) return;

                // 获取深度快照
                var snapshot = provider.GetMarketDepthSnapshot();
                if (snapshot == null) return;

                // 使用 StringBuilder 拼接 DOM 字符串
                StringBuilder sbDom = new StringBuilder(512);
                sbDom.Append("D,");
                sbDom.Append(this.InstrumentInfo.Instrument);
                sbDom.Append(",");

                // Bids
                // 为保证与 ATAS DOM 完全一致，这里不再按 DepthLevels 截断，直接发送 DataFeed 提供的全部可见档位
                var bids = snapshot
                    .Where(x => x.Direction == TradeDirection.Buy)
                    .OrderByDescending(x => x.Price);
                bool first = true;
                foreach (var level in bids)
                {
                    if (!first) sbDom.Append("|");
                    sbDom.Append(level.Price).Append("@").Append(level.Volume);
                    first = false;
                }
                if (first) sbDom.Append("0@0");
                sbDom.Append(",");

                // Asks
                var asks = snapshot
                    .Where(x => x.Direction == TradeDirection.Sell)
                    .OrderBy(x => x.Price);
                first = true;
                foreach (var level in asks)
                {
                    if (!first) sbDom.Append("|");
                    sbDom.Append(level.Price).Append("@").Append(level.Volume);
                    first = false;
                }
                if (first) sbDom.Append("0@0");

                // 交易所时间戳
                sbDom.Append(",");
                sbDom.Append(arg.Time.Ticks);

                string msg = sbDom.ToString();

                // 去重：只有当深度数据真的变化时才发送
                if (msg != _lastDepthHash)
                {
                    // DOM 数据进缓冲区，由 Timer (50ms) 或 阈值触发发送
                    AddToBatch(msg);
                    _lastDepthHash = msg;
                }
            }
            catch { }
        }

        protected override void OnCalculate(int bar, decimal value) { }

        public override void Dispose()
        {
            FlushBatch();
            _flushTimer?.Stop();
            _udpClient?.Close();
            base.Dispose();
        }
    }
}

