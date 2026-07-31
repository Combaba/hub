#!/usr/bin/env python3
"""
统一行情中心 v2 (Market Hub)
============================
1. 从 herzqt ZMQ 订阅实时 tick 行情，保存到 parquet 文件
2. 16:30 下载全市场A股当日 1m/5m/daily 前复权数据 (米筐16:00更新完毕)
3. 23:00-23:59 逐日向前补齐历史缺失数据 (消耗剩余配额，24:00重置)
4. FastAPI 提供 tick/1m/5m/daily 查询接口

架构:
  herzqt:19002 ──SUB──▸ MarketHubV2
                        ├─ ZMQ PUB :19800  [snap]  快照分发(兼容旧客户端)
                        ├─ ZMQ PUB :19801  [bar]   1m K线分发(兼容旧客户端)
                        ├─ ZMQ REP :19802  [query] ZMQ查询(兼容旧客户端)
                        ├─ FastAPI :19803  [http]  Web API
                        └─ 文件保存 tick→parquet

数据存储:
  /data/hb_data/stock_tick/{code}/{YYYY-MM-DD}.parquet     # 实时tick
  /data/hb_data/stock_data/minute/{code}.parquet     # 1分钟K线(米筐,全市场A股)
  /data/hb_data/stock_data/5min/{code}.parquet       # 5分钟K线(米筐,全市场A股)
  /data/hb_data/stock_data/daily/{code}.parquet       # 日线(米筐,全市场A股)

运行:
  python3 market_hub_v2.py [--zmq-host HOST] [--zmq-port PORT]
                           [--snap-port PORT] [--bar-port PORT]
                           [--query-port PORT] [--api-port PORT]
"""

import sys
import os
import json
import time
import argparse
import logging
import signal
import threading
import queue
import asyncio
from datetime import datetime, date, timedelta
from pathlib import Path
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor

import zmq
import numpy as np
import pandas as pd

# ============================================================
# 常量
# ============================================================
DATA_ROOT = Path('/data/hb_data')
TICK_DIR = DATA_ROOT / 'stock_tick'
OPEN_TICK_DIR = DATA_ROOT / 'stock_open_tick'  # 开盘5分钟tick(米筐)
MINUTE_DIR = DATA_ROOT / 'stock_data' / 'minute'
FIVE_MIN_DIR = DATA_ROOT / 'stock_data' / '5min'
DAILY_DIR = DATA_ROOT / 'stock_data' / 'daily'
INDEX_1M_DIR = DATA_ROOT / 'stock_data' / 'index_1m'
LOG_FILE = DATA_ROOT / 'market_hub_v2.log'

# 米筐 License (4个key轮换)
LIC1 = 'ZGZgrvgm8TkwLJecxjgho-019MRIJaOIcE6FaIBM4y7mzMx9XXqkh_fs4gRida8kgHnnfGkmoJ4bgyBNxFD4O9QCLqYtgjNQLA8iP0ytwYkgcs765TcZTDAs-pRclVpZeXTAbsyfmehI2a5-SROZgAmJOkhgysA-NMqaznMj1HY=US_ajXXf4hdOQ5ve32AdVaMqJwIGMX2mCIUbOyMs2VAGfsxftkF3vPNaJ_imjZchCZDAHq69HawaHpJJFDnIk9iHatIJxfeDlO8GTv-UpxuD49MlOFfcgZDBrnKZOrMNZby146HZ3ebrm5QkoOfZEpW6iCNYTb-YDY1tbbqJ5uE='  # 我在用的 2G配额
LIC2 = 'NwIpV8Zpf0etrFRA95xqPeS6ukLJycDLVOsJu-Kh9StUI38TxdO3D16z2QfMUWCmciRiLbUz_Z58eEAtLNbNmNI0LVBmo-_6UH0L0-UQnlTTeLCs7PNu5-Kk5P-2QO55v7SSqdNeqb-I-BJZkNBAux93bzkS1cGHXHBi40ZsWaU=caYXahHdB8-yrzadSGxYLFfGty0jXfqZHAn5Lv4o2dW83qwrD9VIaFOeXVYjPhNpOrvJJmHT-jIQnGITVm9SwCw4Cze_HPoZT9s-uZMB7wHl68W-n6LateJ44WFd6-uQ3dg5opjl5WX6JqQ-woVS3RE5Ot4BRk3mAnzlkf7PZ5w='  # ai量化线上用
LIC3 = 'dshfO8_LaeVa9kS8OC8IcpcasqUzi3xM18w39hSbrs43vREQ6Ptb9ogmaqGtIMAsu3bs8n_xxY6QG-shgxBtryICXoSRx2YL6WWRxXnsKELGPK9ZmK6MeoBhR3yoKPKtjLgmxSR8-R8Ro-fqCq3m3G5dDXu3mibPA_lfnsmL-w8=guULvR3FRHQrftt8ggN4xrJ1MJr0PQkl6Y4i3TYlGwwDBch--YUkXylhHoe9CPSkZ8sHQ4uhFojlDL-1l9h7lqlnmYjkkkL5WG-fVBe0SWEGuDZG72tfFrx9OC_t22FjTzXmIMDqgISLGZG4D2Ltur-XxBEfWjyZkdd6mDpBRzM='  # 多因子投研
LIC4 = 'i8hoAA2awMPbBpOIuLbW_UVknzip3J59SzgwORMKJqAg4Nk91HxblEr_xpgRK4-TFtAagPDC6nbuYMhRdvwt_c9sZmwXge1-MNemf35-HKa1Bnt-W1rmYWBWHy9l3r6ZsH-0y3as6CKicsyTu2ZS-H0qTclFqdPVBaOKrvD-L2I=J7zT3zKtrolJDfJ4yPrlm_klTSsFI3n8XE0ZGWyfV6350L6AWxFLeR_-nfMZ_tLeLbGIiciT4ovjxHepuwNQ-yks4JOvxyeEZ8gtpTQIXnO-mueNIyFwAaBMxGbkl0dAPlERfp33Q4Ry0pb2aLRwI6XWkk4DuTBenhrqgwwmKDc='  # 期权生产
LICENSES = [LIC1, LIC2, LIC3, LIC4]  # 4个key轮换

# 需要下载1m/5m/daily的指数
INDICES = {
    '000001.XSHG': '上证指数', '000300.XSHG': '沪深300',
    '000852.XSHG': '中证1000', '000016.XSHG': '上证50',
    '000905.XSHG': '中证500', '399001.XSHE': '深证成指',
    '399006.XSHE': '创业板指', '399005.XSHE': '中小板指',
    '399673.XSHE': '创业板50',
}

# 行业指数 — 注意: 米筐行业指数用.INDX后缀，非.XSHG
INDUSTRY_INDICES = {
    '801010.INDX': '农林牧渔', '801030.INDX': '采掘', '801040.INDX': '钢铁',
    '801050.INDX': '有色金属', '801080.INDX': '电子', '801110.INDX': '家用电器',
    '801120.INDX': '食品饮料', '801130.INDX': '纺织服装', '801140.INDX': '轻工制造',
    '801150.INDX': '医药生物', '801160.INDX': '公用事业', '801170.INDX': '交通运输',
    '801180.INDX': '房地产', '801200.INDX': '商业贸易', '801210.INDX': '休闲服务',
    '801230.INDX': '综合', '801710.INDX': '建筑材料', '801720.INDX': '建筑装饰',
    '801730.INDX': '电气设备', '801740.INDX': '国防军工', '801750.INDX': '计算机',
    '801760.INDX': '传媒', '801770.INDX': '通信', '801780.INDX': '银行',
    '801790.INDX': '非银金融', '801880.INDX': '汽车', '801890.INDX': '机械设备',
    '801950.INDX': '煤炭', '801960.INDX': '石油石化', '801970.INDX': '环保',
    '801980.INDX': '美容护理',
}

# 已知指数代码 (herzqt推送的SecurityID)
KNOWN_INDICES = {'000001', '000016', '000300', '000852', '000905',
                 '399001', '399005', '399006', '399673'}

# Tick保存间隔(秒) — 每30秒flush一次parquet
TICK_FLUSH_INTERVAL = 30

# 米筐tick数据字段(开盘5分钟)
TICK_FIELDS = [
    'trading_date', 'open', 'last', 'high', 'low', 'prev_close',
    'volume', 'total_turnover',
    'a1', 'a2', 'a3', 'a4', 'a5',
    'b1', 'b2', 'b3', 'b4', 'b5',
    'a1_v', 'a2_v', 'a3_v', 'a4_v', 'a5_v',
    'b1_v', 'b2_v', 'b3_v', 'b4_v', 'b5_v',
    'change_rate', 'num_trades',
]


# ============================================================
# 日志
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.FileHandler(str(LOG_FILE), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger('market_hub_v2')


# ============================================================
# 代码标准化
# ============================================================
def normalize_code(sid, exchange_id=''):
    """herzqt SecurityID + ExchangeID → 标准化代码"""
    if not sid:
        return None
    sid = str(sid).strip()

    # 指数
    if sid in KNOWN_INDICES or sid.startswith('399'):
        return sid + '.IDX'
    if sid.startswith('6'):
        return sid + '.SH'
    if sid.startswith(('0', '3', '4', '8')):
        return sid + '.SZ'
    return None


def is_index(code):
    return code.endswith('.IDX')


def code_to_rqdata(code):
    """标准化代码 → 米筐代码: 600000.SH → 600000.XSHG"""
    if '.' not in code:
        return code
    sym, ex = code.rsplit('.', 1)
    if ex == 'SH':
        return sym + '.XSHG'
    elif ex == 'SZ':
        return sym + '.XSHE'
    elif ex == 'IDX':
        # 指数: 000001.IDX → 000001.XSHG (上证) 或 399001.XSHE (深证)
        if sym.startswith('399'):
            return sym + '.XSHE'
        return sym + '.XSHG'
    return code


# ============================================================
# Tick 文件保存器
# ============================================================
class TickSaver:
    """实时tick数据保存到parquet文件

    目录结构: /data/hb_data/stock_tick/{code}/{YYYY-MM-DD}.parquet
    每30秒flush一次，避免频繁IO
    """

    def __init__(self, flush_interval=TICK_FLUSH_INTERVAL):
        self.flush_interval = flush_interval
        # code -> list of tick dicts (待保存buffer)
        self._buffers = defaultdict(list)
        self._lock = threading.Lock()
        self._last_flush = time.time()
        self._tick_count = 0
        self._file_count = 0
        self._current_date = date.today().isoformat()

    def add(self, code: str, tick: dict):
        """添加一条tick到buffer"""
        with self._lock:
            self._buffers[code].append(tick)
            self._tick_count += 1

        # 检查是否需要flush
        now = time.time()
        if now - self._last_flush >= self.flush_interval:
            self.flush()

    def flush(self):
        """将所有buffer写入parquet文件"""
        with self._lock:
            if not self._buffers:
                return
            buffers = dict(self._buffers)
            self._buffers.clear()
            self._last_flush = time.time()
            today = self._current_date

        for code, ticks in buffers.items():
            if not ticks:
                continue
            try:
                self._save_ticks(code, today, ticks)
                self._file_count += 1
            except Exception as e:
                log.error(f"Tick保存失败 {code}: {e}")

    def _save_ticks(self, code: str, day: str, ticks: list):
        """保存一批tick到parquet"""
        df = pd.DataFrame(ticks)
        out_dir = TICK_DIR / code
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f'{day}.parquet'

        # 如果文件已存在，追加
        if out_file.exists():
            existing = pd.read_parquet(out_file)
            df = pd.concat([existing, df], ignore_index=True)
            # 去重(按timestamp+code)
            df = df.drop_duplicates(subset=['timestamp', 'code'], keep='last')

        df.to_parquet(out_file, index=False)

    def get_tick_data(self, code: str, day: str = None, start_time: str = None,
                      end_time: str = None, limit: int = 1000) -> list:
        """查询tick数据"""
        if day is None:
            day = date.today().isoformat()

        out_file = TICK_DIR / code / f'{day}.parquet'
        if not out_file.exists():
            return []

        df = pd.read_parquet(out_file)

        # 时间过滤
        if start_time:
            df = df[df['timestamp'] >= start_time]
        if end_time:
            df = df[df['timestamp'] <= end_time]

        if limit > 0:
            df = df.tail(limit)

        return df.to_dict('records')

    def stats(self):
        return {
            'tick_count': self._tick_count,
            'file_count': self._file_count,
            'buffered_codes': len(self._buffers),
        }


# ============================================================
# 1分钟K线聚合器
# ============================================================
class BarAggregator:
    """从快照聚合1分钟/5分钟K线"""

    def __init__(self, max_bars=500):
        self.max_bars = max_bars
        self._builders = {}
        self._completed_1m = {}  # code -> deque
        self._completed_5m = {}  # code -> deque
        self._lock = threading.Lock()

    def update(self, code, price, volume, turnover, timestamp_str,
               pre_close=0, open_price=0, high_price=0, low_price=0,
               ask1=0, bid1=0):
        """更新快照，自动聚合1分钟和5分钟K线"""
        try:
            parts = timestamp_str.split(':')
            if len(parts) < 2:
                return None, None
            hh, mm = int(parts[0]), int(parts[1])
            bar_key_1m = hh * 60 + mm
            bar_key_5m = hh * 60 + (mm // 5) * 5
        except (ValueError, IndexError):
            return None, None

        today = date.today()
        bar_time_1m = datetime(today.year, today.month, today.day, hh, mm, 0)
        bar_time_5m = datetime(today.year, today.month, today.day,
                               bar_key_5m // 60, bar_key_5m % 60, 0)

        completed_1m = None
        completed_5m = None

        with self._lock:
            # --- 1分钟K线 ---
            if code not in self._builders:
                self._builders[code] = {
                    'key_1m': -1, 'key_5m': -1,
                    'bar_1m': None, 'bar_5m': None,
                    'prev_cum_vol': 0, 'prev_cum_to': 0,
                }
                self._completed_1m[code] = deque(maxlen=self.max_bars)
                self._completed_5m[code] = deque(maxlen=self.max_bars)

            b = self._builders[code]
            delta_vol = max(0, volume - b['prev_cum_vol'])
            delta_to = max(0, turnover - b['prev_cum_to'])

            # 1分钟bar
            if b['key_1m'] != bar_key_1m:
                if b['bar_1m'] is not None:
                    completed_1m = b['bar_1m'].copy()
                    self._completed_1m[code].append(completed_1m)
                b['key_1m'] = bar_key_1m
                b['bar_1m'] = {
                    'code': code, 'datetime': bar_time_1m.isoformat(),
                    'open': price if open_price <= 0 else open_price,
                    'high': price if high_price <= 0 else high_price,
                    'low': price if low_price <= 0 else low_price,
                    'close': price,
                    'volume': delta_vol, 'turnover': delta_to,
                    'pre_close': pre_close, 'ask1': ask1, 'bid1': bid1,
                }
                b['prev_cum_vol'] = volume
                b['prev_cum_to'] = turnover
            else:
                bar = b['bar_1m']
                bar['close'] = price
                if high_price > 0 and high_price > bar['high']:
                    bar['high'] = high_price
                if low_price > 0 and (bar['low'] <= 0 or low_price < bar['low']):
                    bar['low'] = low_price
                if volume > b['prev_cum_vol']:
                    bar['volume'] += volume - b['prev_cum_vol']
                if turnover > b['prev_cum_to']:
                    bar['turnover'] += turnover - b['prev_cum_to']
                b['prev_cum_vol'] = volume
                b['prev_cum_to'] = turnover
                if ask1 > 0:
                    bar['ask1'] = ask1
                if bid1 > 0:
                    bar['bid1'] = bid1

            # 5分钟bar
            if b['key_5m'] != bar_key_5m:
                if b['bar_5m'] is not None:
                    completed_5m = b['bar_5m'].copy()
                    self._completed_5m[code].append(completed_5m)
                b['key_5m'] = bar_key_5m
                b['bar_5m'] = {
                    'code': code, 'datetime': bar_time_5m.isoformat(),
                    'open': price if open_price <= 0 else open_price,
                    'high': price if high_price <= 0 else high_price,
                    'low': price if low_price <= 0 else low_price,
                    'close': price,
                    'volume': delta_vol, 'turnover': delta_to,
                    'pre_close': pre_close,
                }
            else:
                bar5 = b['bar_5m']
                bar5['close'] = price
                if high_price > 0 and high_price > bar5['high']:
                    bar5['high'] = high_price
                if low_price > 0 and (bar5['low'] <= 0 or low_price < bar5['low']):
                    bar5['low'] = low_price
                if volume > b['prev_cum_vol']:
                    bar5['volume'] += volume - b['prev_cum_vol']
                if turnover > b['prev_cum_to']:
                    bar5['turnover'] += turnover - b['prev_cum_to']

        return completed_1m, completed_5m

    def get_1m_bars(self, code, count=120):
        with self._lock:
            completed = list(self._completed_1m.get(code, []))
            current = self._builders.get(code, {}).get('bar_1m')
            if current:
                completed.append(current)
            return completed[-count:]

    def get_5m_bars(self, code, count=48):
        with self._lock:
            completed = list(self._completed_5m.get(code, []))
            current = self._builders.get(code, {}).get('bar_5m')
            if current:
                completed.append(current)
            return completed[-count:]


# ============================================================
# 米筐数据日更模块 — 全市场A股 + 逐日补齐 + 多Key轮换
# ============================================================
class RqDataUpdater:
    """米筐数据下载 — 两个定时任务:

    1. 16:30 下载当日最新数据 (米筐16:00左右更新完毕)
       - 全市场A股当日 1m/5m/daily 前复权数据
       - 指数当日1m数据

    2. 23:00-23:59 逐日向前补齐历史缺失数据 (24:00配额重置，消耗剩余配额)
       - 检查每个文件最后日期，从缺失日开始逐日向前补
       - 配额耗尽即停，次日23:00继续
       - 补齐起始: 2020-01-01
    """

    # 逐日补齐起始日期
    BACKFILL_START = '2000-01-01'

    def __init__(self):
        self._rq = None
        self._last_today_download = None   # 16:30当日下载时间
        self._last_backfill = None          # 23:00补齐时间
        self._running = False
        self._current_key_idx = 0
        self._all_stocks = []  # 全市场A股列表
        self._progress = {}    # 下载进度

    def _init_rqdata(self, license_idx=0):
        """初始化米筐连接"""
        import rqdatac as rq
        try:
            rq.disconnect()
        except Exception:
            pass
        lic = LICENSES[license_idx]
        uri = f'tcp://license:{lic}@rqdatad-pro.ricequant.com:16011'
        os.environ['RQDATAC2_CONF'] = uri
        os.environ['RQSDK_LICENSE'] = uri
        rq.init()
        quota = rq.user.get_quota()
        used_mb = quota['bytes_used'] / (1024**2)
        limit_mb = quota['bytes_limit'] / (1024**2)
        pct = used_mb / limit_mb * 100
        log.info(f"米筐Key{license_idx+1}/{len(LICENSES)} 配额: {used_mb:.0f}/{limit_mb:.0f} MB ({pct:.1f}%)")
        self._rq = rq
        self._current_key_idx = license_idx
        return rq

    def _switch_key_if_needed(self, threshold=0.80):
        """检查Key1配额，超过阈值停止下载(不轮换)
        
        返回: False=配额不足应停止下载
        """
        try:
            quota = self._rq.user.get_quota()
            pct = quota['bytes_used'] / quota['bytes_limit']
            if pct > threshold:
                log.error(f"⚠ Key1配额{pct*100:.1f}%超过{threshold*100:.0f}%阈值，停止下载(不轮换)")
                return False
            return False
        except Exception as e:
            log.error(f"配额检查异常: {e}")
            return False

    def _get_all_stocks(self, rq):
        """获取全市场A股列表(过滤退市) — 固定Key1"""
        try:
            all_inst = rq.all_instruments('CS')
            # 用status过滤: 只保留Active状态的股票
            # (de_listed_date是str类型,退市用'0000-00-00',isna()无效)
            if 'status' in all_inst.columns:
                active = all_inst[all_inst['status'] == 'Active']
                stocks = active['order_book_id'].tolist()
            else:
                # fallback: 过滤de_listed_date为空或'0000-00-00'的
                stocks = all_inst['order_book_id'].tolist()
                if 'de_listed_date' in all_inst.columns:
                    today = date.today().isoformat()
                    mask = (all_inst['de_listed_date'] == '0000-00-00') | \
                           (all_inst['de_listed_date'] > today)
                    stocks = all_inst.loc[mask, 'order_book_id'].tolist()
            log.info(f"全市场A股: {len(stocks)}只 (活跃)")
            return stocks
        except Exception as e:
            err_str = str(e)[:200]
            log.error(f"获取全市场A股列表失败: {err_str}")
        return []

    def _get_last_date_from_parquet(self, filepath):
        """从parquet文件获取最后交易日日期"""
        if not filepath.exists():
            return None
        try:
            df = pd.read_parquet(filepath, columns=['datetime'])
            if df.empty:
                return None
            last_dt = pd.to_datetime(df['datetime'].iloc[-1])
            return last_dt.strftime('%Y-%m-%d')
        except Exception:
            return None

    def _get_first_date_from_parquet(self, filepath):
        """从parquet文件获取最早交易日日期(R8新增: 用于向前回溯补齐)"""
        if not filepath.exists():
            return None
        try:
            df = pd.read_parquet(filepath, columns=['datetime'])
            if df.empty:
                return None
            first_dt = pd.to_datetime(df['datetime'].iloc[0])
            return first_dt.strftime('%Y-%m-%d')
        except Exception:
            return None

    def _find_stock_file(self, rq_code, data_dir, suffix=''):
        """查找股票对应的parquet文件(模糊匹配文件名)"""
        prefix = rq_code.replace('.XSHG', '_XSHG').replace('.XSHE', '_XSHE').replace('.INDX', '_INDX')
        import glob as glob_mod
        exact = data_dir / f'{prefix}{suffix}.parquet'
        if exact.exists():
            return exact
        candidates = glob_mod.glob(str(data_dir / f'{prefix}*.parquet'))
        if candidates:
            return Path(candidates[0])
        return None

    # ============================================================
    # 数据完整性检查
    # ============================================================
    def _check_data_integrity(self, df, freq, rq_code):
        """下载后数据完整性验证

        检查项:
          1. 非空检查 — 空DataFrame直接拒绝
          2. NaN比例 — OHLCV任一字段NaN>5%拒绝, >1%告警
          3. 价格合理性 — close>0, high>=low, volume>=0
          4. 行数检查 — 1m每日应~240行, 5m每日~48行, 1d每日1行
        返回: dict(valid, row_count, nan_pct, bad_price_count, warnings)
        """
        result = {
            'valid': False, 'row_count': 0, 'nan_pct': 100.0,
            'bad_price_count': 0, 'warnings': []
        }

        # 1. 空检查
        if df is None or df.empty:
            result['warnings'].append('空DataFrame')
            return result

        result['row_count'] = len(df)

        # 2. NaN检查
        ohlcv_cols = [c for c in ['open', 'high', 'low', 'close', 'volume', 'total_turnover']
                      if c in df.columns]
        if ohlcv_cols:
            nan_count = df[ohlcv_cols].isna().sum().sum()
            total_cells = len(df) * len(ohlcv_cols)
            result['nan_pct'] = (nan_count / total_cells * 100) if total_cells > 0 else 0.0

            if result['nan_pct'] > 5.0:
                result['warnings'].append(f'NaN比例{result["nan_pct"]:.1f}%>5%, 拒绝写入')
                return result
            elif result['nan_pct'] > 1.0:
                result['warnings'].append(f'NaN比例{result["nan_pct"]:.1f}%>1%, 告警')

        # 3. 价格合理性
        bad = 0
        if 'close' in df.columns:
            bad += (df['close'] <= 0).sum()
        if 'high' in df.columns and 'low' in df.columns:
            bad += (df['high'] < df['low']).sum()
        if 'volume' in df.columns:
            bad += (df['volume'] < 0).sum()
        result['bad_price_count'] = int(bad)
        if bad > len(df) * 0.01:  # >1%坏数据
            result['warnings'].append(f'价格异常{bad}行>{1}%, 拒绝写入')
            return result

        # 4. 行数合理性(宽松检查, 只在行数极少时告警)
        expected = {'1m': 200, '5m': 40, '1d': 1}
        if freq in expected and len(df) < expected[freq]:
            result['warnings'].append(f'行数{len(df)}<预期{expected[freq]}')

        result['valid'] = True
        return result

    # ============================================================
    # 增量更新: 计算缺失段
    # ============================================================
    def _compute_missing_ranges(self, rq_code, data_dir, freq,
                                backfill_start=None, max_days_per_range=30):
        """计算单只股票的缺失日期段(含向前回溯补齐)

        逻辑:
          1. 无文件 → [BACKFILL_START → 今天], 分30天一段
          2. 有文件 → 两部分:
             a) 向前补齐: BACKFILL_START → 文件最早日期(如果最早日期>BACKFILL_START)
             b) 向后补齐: 文件最后日期+1 → 今天
          3. 每段最多max_days_per_range天
          4. 23:00补齐阶段: 优先向后(当日缺口)，再向前(历史缺口)
        
        R8修复: 原逻辑只补last_date→今天，不向前回溯到2020-01-01
        导致每天只补当天数据，87.5%配额浪费
        """
        if backfill_start is None:
            backfill_start = self.BACKFILL_START
        today_str = date.today().isoformat()

        f = self._find_stock_file(rq_code, data_dir)
        if not f:
            last_date = None
            first_date = None
        else:
            last_date = self._get_last_date_from_parquet(f)
            first_date = self._get_first_date_from_parquet(f)

        ranges = []

        # === 向后补齐: last_date+1 → 今天 ===
        if last_date:
            start = (pd.to_datetime(last_date) + timedelta(days=1)).strftime('%Y-%m-%d')
            if start <= today_str:
                current = pd.to_datetime(start)
                end_target = pd.to_datetime(today_str)
                while current < end_target:
                    seg_end = min(current + timedelta(days=max_days_per_range), end_target)
                    ranges.append((
                        current.strftime('%Y-%m-%d'),
                        seg_end.strftime('%Y-%m-%d')
                    ))
                    current = seg_end + timedelta(days=1)
        else:
            # 无文件: 从BACKFILL_START到今天
            current = pd.to_datetime(backfill_start)
            end_target = pd.to_datetime(today_str)
            while current < end_target:
                seg_end = min(current + timedelta(days=max_days_per_range), end_target)
                ranges.append((
                    current.strftime('%Y-%m-%d'),
                    seg_end.strftime('%Y-%m-%d')
                ))
                current = seg_end + timedelta(days=1)

        # === 向前补齐: BACKFILL_START → first_date(如果first_date存在且>BACKFILL_START) ===
        if first_date and last_date:
            first_dt = pd.to_datetime(first_date)
            backfill_dt = pd.to_datetime(backfill_start)
            if first_dt > backfill_dt:
                # 从BACKFILL_START到first_date-1天
                current = backfill_dt
                end_target = first_dt - timedelta(days=1)
                forward_ranges = []
                while current <= end_target:
                    seg_end = min(current + timedelta(days=max_days_per_range), end_target)
                    forward_ranges.append((
                        current.strftime('%Y-%m-%d'),
                        seg_end.strftime('%Y-%m-%d')
                    ))
                    current = seg_end + timedelta(days=1)
                # 向前补齐段追加到末尾(优先向后补齐当天缺口)
                ranges.extend(forward_ranges)

        return ranges

    # ============================================================
    # 任务1: 16:30 下载当日最新数据
    # ============================================================
    def download_today_update(self):
        """16:30触发 — 下载全市场A股当日 1m/5m/daily 前复权数据"""
        if self._running:
            log.warning("米筐任务已在运行中，跳过当日下载")
            return False
        self._running = True

        # 固定使用Key1(2G配额), 不轮换
        rq = None
        try:
            rq = self._init_rqdata(0)
            log.info(f"✅ 使用Key1(2G配额)进行当日下载")
        except Exception as e:
            log.error(f"❌ Key1连接失败: {e}")
            self._running = False
            return False

        today_str = date.today().isoformat()

        # 获取最近交易日 — 米筐日线T+1更新，当日无日线
        # 1m/5m当日有实时数据，日线用倒数第二个交易日(已确认有数据)
        try:
            trading_dates = rq.get_trading_dates(date.today() - timedelta(days=10), date.today())
            if len(trading_dates) >= 2:
                # 日线T+1: 取倒数第二个交易日(已收盘确认)
                last_daily_day = trading_dates[-2].isoformat()
            elif trading_dates:
                last_daily_day = trading_dates[-1].isoformat()
            else:
                last_daily_day = today_str
        except Exception:
            last_daily_day = today_str
        log.info(f"  今日={today_str}, 日线最近交易日={last_daily_day}")

        # 获取全市场A股列表
        self._all_stocks = self._get_all_stocks(rq)
        if not self._all_stocks:
            log.error("无法获取A股列表，终止当日下载")
            self._running = False
            return False

        log.info(f"📊 米筐当日下载开始: 全市场{len(self._all_stocks)}只A股 + 指数")
        success = True

        try:
            # Phase 1: 指数1m数据(大盘+行业) — 当日
            self._progress['phase'] = 'today_index_1m'
            self._download_index_1m(rq, today_str)

            # Phase 2: 全市场A股当日1m数据
            self._progress['phase'] = 'today_stock_1m'
            self._download_stock_today(rq, '1m', MINUTE_DIR, self._all_stocks, today_str)

            # Phase 3: 全市场A股当日5m数据
            self._progress['phase'] = 'today_stock_5m'
            self._download_stock_today(rq, '5m', FIVE_MIN_DIR, self._all_stocks, today_str)

            # Phase 4: 全市场A股日线数据 — 用最近交易日(米筐日线T+1)
            self._progress['phase'] = 'today_stock_daily'
            self._download_stock_today(rq, '1d', DAILY_DIR, self._all_stocks, last_daily_day)

        except Exception as e:
            log.error(f"米筐当日下载异常: {e}")
            import traceback
            log.error(traceback.format_exc())
            success = False

        self._last_today_download = datetime.now()
        self._progress['phase'] = 'today_done'
        self._running = False
        log.info(f"📊 米筐当日下载完成, 成功={success}")
        return success

    def _download_stock_today(self, rq, freq, data_dir, stock_list, today_str):
        """下载全市场A股当日数据(只取当天)"""
        freq_label = {'1m': '1分钟', '5m': '5分钟', '1d': '日线'}[freq]
        log.info(f"  下载{freq_label}当日数据: {len(stock_list)}只A股")
        data_dir.mkdir(parents=True, exist_ok=True)
        batch_size = 20 if freq == '1m' else 50

        done = 0
        failed = 0
        total_batches = (len(stock_list) - 1) // batch_size + 1

        for i in range(0, len(stock_list), batch_size):
            batch = stock_list[i:i + batch_size]
            batch_num = i // batch_size + 1

            try:
                df = rq.get_price(
                    batch,
                    start_date=today_str,
                    end_date=today_str,
                    frequency=freq,
                    fields=['open', 'high', 'low', 'close', 'volume', 'total_turnover'],
                    expect_df=True,
                    adjust_type='pre'  # 前复权
                )

                if df is not None and not df.empty:
                    df = df.reset_index()
                    for rq_code in batch:
                        stock_df = df[df['order_book_id'] == rq_code]
                        if stock_df.empty:
                            continue
                        self._save_stock_parquet(rq_code, stock_df, data_dir)
                        done += 1
                else:
                    failed += len(batch)

            except Exception as e:
                err_str = str(e)[:150]
                # 配额耗尽/连接数超限 → 尝试切换Key
                if 'quota' in err_str.lower() or 'limit' in err_str.lower() or 'login machine' in err_str.lower():
                    if not self._switch_key_if_needed(0.5):
                        log.error(f"  ⚠ Key1配额耗尽，停止当日下载 — 最后错误: {err_str}")
                        break
                    continue  # 切换成功,重试本批次
                else:
                    log.error(f"    {freq_label}批次{batch_num}/{total_batches}失败: {err_str}")
                    failed += len(batch)

            if batch_num % 50 == 0 or batch_num == total_batches:
                log.info(f"  {freq_label}当日进度: {batch_num}/{total_batches}批 "
                         f"(完成{done} 失败{failed})")
                self._switch_key_if_needed()

            time.sleep(0.2)

        log.info(f"  ✓ {freq_label}当日下载完成: 成功{done} 失败{failed}")

    # ============================================================
    # 任务2: 23:00-23:59 逐日向前补齐历史缺失数据 (按天为粒度)
    # ============================================================
    def backfill_history(self):
        """23:00触发 — 消耗剩余配额，逐日向前补齐历史缺失数据

        按天为粒度补齐(非按股票):
          1. 扫描全市场5201只股票parquet，收集全局缺失日期
          2. 按天从近到远排序: 先补最近缺失日，再补前1天...
          3. 每天批量下载全市场1m/5m/daily，3个频率同天一起补
          4. 配额耗尽即停，次日23:00继续
          5. 24:00配额重置，所以23:00-23:59尽量消耗完

        优势: 每天数据完整性优先，而非单只股票完整性优先
        """
        if self._running:
            log.warning("米筐任务已在运行中，跳过历史补齐")
            return False
        self._running = True

        # 固定使用Key1(2G配额), 不轮换
        rq = None
        try:
            rq = self._init_rqdata(0)
            log.info(f"✅ 使用Key1(2G配额)进行历史补齐")
        except Exception as e:
            log.error(f"❌ Key1连接失败: {e}")
            self._running = False
            return False

        today_str = date.today().isoformat()

        # 获取全市场A股列表(复用或重新获取，配额超限自动切换key)
        if not self._all_stocks:
            self._all_stocks = self._get_all_stocks(rq)
        if not self._all_stocks:
            log.error("无法获取A股列表(Key1配额耗尽)，终止历史补齐")
            self._running = False
            return False

        # 获取交易日历(从BACKFILL_START到今天)
        try:
            all_trading_days = rq.get_trading_dates(
                pd.to_datetime(self.BACKFILL_START), date.today()
            )
            trading_day_strs = [d.strftime('%Y-%m-%d') for d in all_trading_days]
        except Exception as e:
            log.error(f"获取交易日历失败: {e}")
            self._running = False
            return False

        log.info(f"📊 米筐历史补齐开始: 全市场{len(self._all_stocks)}只A股 "
                 f"(交易日历{len(trading_day_strs)}天, 按天为粒度)")

        success = True

        try:
            # Phase 1: 指数1m历史补齐(指数数据量小，快速完成)
            self._progress['phase'] = 'backfill_index_1m'
            self._download_index_1m(rq, today_str)

            # Phase 2: 扫描全市场缺失日期(3个频率一起扫描)
            self._progress['phase'] = 'scanning_missing_days'
            missing_days = self._scan_missing_days(trading_day_strs)

            if not missing_days:
                log.info("📊 全市场数据已完整，无需补齐")
            else:
                log.info(f"📊 缺失日期: 共{len(missing_days)}天, "
                         f"从{missing_days[0]}到{missing_days[-1]}")

                # Phase 3: 按天补齐 — 从最近缺失日往远补
                self._progress['phase'] = 'backfill_by_day'
                self._backfill_by_day(rq, missing_days, self._all_stocks)

            # Phase 4: 开盘5分钟tick — 已禁用(用户要求去掉tick下载)
            # self._progress['phase'] = 'backfill_open_tick'
            # self._download_open_tick(rq, self._all_stocks)

        except Exception as e:
            log.error(f"米筐历史补齐异常: {e}")
            import traceback
            log.error(traceback.format_exc())
            success = False

        self._last_backfill = datetime.now()
        self._progress['phase'] = 'backfill_done'
        self._running = False
        log.info(f"📊 米筐历史补齐完成, 成功={success}")
        return success

    def _scan_missing_days(self, trading_day_strs):
        """扫描全市场parquet文件，收集全局缺失日期(3个频率取并集)

        返回: 缺失日期列表，从近到远排序(最近的缺失日排在前面)
        逻辑:
          1. 对每个频率(1m/5m/daily)，扫描目录下所有parquet文件
          2. 收集每个日期出现的股票数(日期→股票数映射)
          3. 缺失日 = 覆盖股票数 < 90%总股票数 的日期
          4. 3个频率取并集: 任一频率缺失即算缺失
          5. 从近到远排序: 先补最近的缺失日
        """
        all_stocks = self._all_stocks
        total_stocks = len(all_stocks)
        coverage_threshold = int(total_stocks * 0.9)  # 90%覆盖才算完整

        # 3个频率的缺失日期集合
        missing_by_freq = {}

        for freq, data_dir in [('1m', MINUTE_DIR), ('5m', FIVE_MIN_DIR), ('1d', DAILY_DIR)]:
            freq_label = {'1m': '1分钟', '5m': '5分钟', '1d': '日线'}[freq]
            log.info(f"  扫描{freq_label}目录: {data_dir}")

            # 日期→覆盖股票数
            date_stock_count = defaultdict(int)
            scanned = 0

            # 直接遍历目录下所有parquet文件(比逐股查找更快)
            if data_dir.exists():
                parquet_files = list(data_dir.glob('*.parquet'))
                log.info(f"    {freq_label}: 发现{len(parquet_files)}个文件")

                for pf in parquet_files:
                    try:
                        df = pd.read_parquet(pf, columns=['datetime'])
                        if df.empty:
                            continue
                        dates = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d').unique()
                        for d in dates:
                            date_stock_count[d] += 1
                    except Exception:
                        continue

                    scanned += 1
                    if scanned % 1000 == 0:
                        log.info(f"    {freq_label}扫描进度: {scanned}/{len(parquet_files)}")

            # 缺失日 = 覆盖股票数 < 90% 的交易日
            missing_days = set()
            for day in trading_day_strs:
                if date_stock_count.get(day, 0) < coverage_threshold:
                    missing_days.add(day)

            missing_by_freq[freq] = missing_days
            covered_days = len([d for d in trading_day_strs if date_stock_count.get(d, 0) >= coverage_threshold])
            log.info(f"  {freq_label}: 完整{covered_days}天(≥{coverage_threshold}只), "
                     f"缺失{len(missing_days)}天(<{coverage_threshold}只)")

        # 3个频率取并集: 任一频率缺失即算缺失
        all_missing = set()
        for freq_missing in missing_by_freq.values():
            all_missing.update(freq_missing)

        # 从近到远排序(最近的缺失日排在前面)
        result = sorted(all_missing, reverse=True)
        return result

    def _backfill_by_day(self, rq, missing_days, stock_list):
        """按天为粒度补齐 — 每天批量下载全市场1m/5m/daily

        逻辑:
          1. 从最近的缺失日开始(如2026-07-20)
          2. 每天批量下载全市场5201只的1m/5m/daily
          3. 3个频率同天一起补完，再往前1天
          4. 配额耗尽即停
        """
        total_days = len(missing_days)
        total_stocks = len(stock_list)
        log.info(f"  开始按天补齐: {total_days}天 × {total_stocks}只 × 3频率")

        day_done = 0
        day_failed = 0
        total_stocks_done = 0
        total_stocks_failed = 0

        for day_idx, day_str in enumerate(missing_days):
            day_num = day_idx + 1

            # 3个频率同天一起补
            for freq, data_dir in [('1m', MINUTE_DIR), ('5m', FIVE_MIN_DIR), ('1d', DAILY_DIR)]:
                freq_label = {'1m': '1分钟', '5m': '5分钟', '1d': '日线'}[freq]
                batch_size = 20 if freq == '1m' else 50

                stocks_done, stocks_failed = self._download_day_freq(
                    rq, freq, data_dir, stock_list, day_str, batch_size
                )
                total_stocks_done += stocks_done
                total_stocks_failed += stocks_failed

                # 配额耗尽检查
                if not self._running:
                    log.info(f"  ⚠ 配额耗尽，停止补齐(已完成{day_num}/{total_days}天)")
                    return

            day_done += 1
            # 进度日志(每天1条)
            remaining_days = total_days - day_num
            log.info(f"  📅 补齐进度: 第{day_num}/{total_days}天({day_str}) "
                     f"完成{total_stocks_done}股次 失败{total_stocks_failed}股次 "
                     f"剩余{remaining_days}天")

            # 每5天检查配额
            if day_num % 5 == 0:
                self._switch_key_if_needed()

            time.sleep(0.1)

        log.info(f"  ✓ 按天补齐完成: {day_done}天 成功{total_stocks_done}股次 "
                 f"失败{total_stocks_failed}股次")

    def _download_day_freq(self, rq, freq, data_dir, stock_list, day_str, batch_size):
        """下载全市场指定日期+频率的数据(批量)

        返回: (done_count, failed_count)
        """
        freq_label = {'1m': '1分钟', '5m': '5分钟', '1d': '日线'}[freq]
        data_dir.mkdir(parents=True, exist_ok=True)

        done = 0
        failed = 0
        total_batches = (len(stock_list) - 1) // batch_size + 1

        for i in range(0, len(stock_list), batch_size):
            batch = stock_list[i:i + batch_size]
            batch_num = i // batch_size + 1

            try:
                df = rq.get_price(
                    batch,
                    start_date=day_str,
                    end_date=day_str,
                    frequency=freq,
                    fields=['open', 'high', 'low', 'close', 'volume', 'total_turnover'],
                    expect_df=True,
                    adjust_type='pre'  # 前复权
                )

                if df is not None and not df.empty:
                    df = df.reset_index()
                    saved = 0
                    for rq_code in batch:
                        stock_df = df[df['order_book_id'] == rq_code]
                        if stock_df.empty:
                            continue
                        # 完整性检查
                        integrity = self._check_data_integrity(stock_df, freq, rq_code)
                        if integrity['valid']:
                            self._save_stock_parquet(rq_code, stock_df, data_dir)
                            saved += 1
                        else:
                            log.warning(f"    ✗ {rq_code} {freq_label}[{day_str}] "
                                        f"完整性拒绝: {integrity['warnings']}")
                            failed += 1
                    done += saved
                # 空数据不算失败(可能是非交易日/停牌)，跳过即可

            except Exception as e:
                err_str = str(e)[:150]
                # 配额耗尽/连接数超限 → 尝试切换Key
                if 'quota' in err_str.lower() or 'limit' in err_str.lower() or 'login machine' in err_str.lower():
                    if not self._switch_key_if_needed(0.5):
                        log.error(f"  ⚠ Key1配额耗尽，停止补齐(明日23:00继续) — 最后错误: {err_str}")
                        self._running = False
                        return done, failed
                    # 切换成功,继续重试本批次
                    continue
                else:
                    log.error(f"    {freq_label}[{day_str}]批次{batch_num}/{total_batches}失败: {err_str}")
                    failed += len(batch)

            time.sleep(0.2)

        return done, failed

    def _download_index_1m(self, rq, end_date):
        """下载大盘指数+行业指数1分钟数据(增量: 从已有最后日期开始)"""
        all_indices = {**INDICES, **INDUSTRY_INDICES}
        log.info(f"  下载指数1m: {len(all_indices)}个指数")
        INDEX_1M_DIR.mkdir(parents=True, exist_ok=True)

        for rq_code, name in all_indices.items():
            out_name = rq_code.replace('.XSHG', '_XSHG').replace('.XSHE', '_XSHE').replace('.INDX', '_INDX') + '_1m.parquet'
            out_file = INDEX_1M_DIR / out_name
            last_date = self._get_last_date_from_parquet(out_file)
            start_date = last_date or self.BACKFILL_START

            if start_date >= end_date:
                continue

            try:
                df = rq.get_price(rq_code, start_date=start_date, end_date=end_date,
                                  frequency='1m',
                                  fields=['open', 'high', 'low', 'close',
                                          'volume', 'total_turnover'],
                                  expect_df=True)
                if df is None or df.empty:
                    continue

                df = df.reset_index()
                df['order_book_id'] = rq_code

                if out_file.exists():
                    existing = pd.read_parquet(out_file)
                    df = pd.concat([existing, df], ignore_index=True)
                    df = df.drop_duplicates(subset=['datetime'], keep='last')

                df.to_parquet(out_file, index=False)
                log.info(f"    ✓ {rq_code}({name}): {len(df)}条 [{start_date}→{end_date}]")
            except Exception as e:
                log.error(f"    ✗ {rq_code}({name}): {e}")

            self._switch_key_if_needed()

    def _backfill_stock_data(self, rq, freq, data_dir, stock_list):
        """逐股增量补齐历史数据 — 每只股票只下载缺失段 + 完整性检查

        逻辑:
          1. 对每只股票计算缺失日期段(_compute_missing_ranges)
          2. 按段下载，下载后完整性检查(_check_data_integrity)
          3. 检查通过才写入，不合格丢弃并告警
          4. 配额耗尽即停，次日23:00继续
        """
        freq_label = {'1m': '1分钟', '5m': '5分钟', '1d': '日线'}[freq]
        log.info(f"  开始{freq_label}增量补齐: {len(stock_list)}只A股")

        data_dir.mkdir(parents=True, exist_ok=True)
        today_str = date.today().isoformat()
        batch_size = 20 if freq == '1m' else 50

        # Phase A: 计算每只股票的缺失段
        stock_ranges = {}  # rq_code -> [(start, end), ...]
        total_segments = 0

        for rq_code in stock_list:
            ranges = self._compute_missing_ranges(rq_code, data_dir, freq)
            if ranges:
                stock_ranges[rq_code] = ranges
                total_segments += len(ranges)

        if not stock_ranges:
            log.info(f"  ✓ {freq_label}全部已是最新")
            return

        log.info(f"  {freq_label}: {len(stock_ranges)}只需补齐, 共{total_segments}段")

        # Phase B: 逐股批量下载所有缺失段，最后一次性保存
        # R8优化: 避免每段都读+合并整个文件(78段=78次读+写)
        done = 0
        failed = 0
        rejected = 0  # 完整性检查拒绝
        stock_list_todo = list(stock_ranges.keys())
        total_stocks = len(stock_list_todo)
        backfill_start = self.BACKFILL_START

        log.info(f"  {freq_label}: 开始逐股补齐 {total_stocks}只 (配额耗尽即停，次日继续)")

        for stock_idx, rq_code in enumerate(stock_list_todo):
            stock_num = stock_idx + 1
            seg_list = stock_ranges[rq_code]
            
            # R8优化: 先下载该股所有段到内存，最后一次性合并保存
            stock_dfs = []
            stock_rejected = 0
            
            for seg_start, seg_end in seg_list:
                try:
                    df = rq.get_price(
                        rq_code,
                        start_date=seg_start,
                        end_date=seg_end,
                        frequency=freq,
                        fields=['open', 'high', 'low', 'close', 'volume', 'total_turnover'],
                        expect_df=True,
                        adjust_type='pre'  # 前复权
                    )

                    if df is None or df.empty:
                        continue

                    df = df.reset_index()
                    df['order_book_id'] = rq_code

                    # ★ 完整性检查
                    integrity = self._check_data_integrity(df, freq, rq_code)
                    if not integrity['valid']:
                        log.warning(f"    ✗ {rq_code} {freq_label}[{seg_start}→{seg_end}] "
                                    f"完整性检查拒绝: {integrity['warnings']}")
                        rejected += 1
                        stock_rejected += 1
                        continue

                    if integrity['warnings']:
                        log.warning(f"    ⚠ {rq_code} {freq_label}[{seg_start}→{seg_end}] "
                                    f"完整性告警: {integrity['warnings']}")

                    stock_dfs.append(df)
                    done += 1

                except Exception as e:
                    err_str = str(e)[:100]
                    # 配额耗尽/连接数超限 → 尝试切换Key
                    if 'quota' in err_str.lower() or 'limit' in err_str.lower() or 'login machine' in err_str.lower():
                        if not self._switch_key_if_needed(0.5):
                            log.error(f"  ⚠ Key1配额耗尽，停止历史补齐(明日23:00继续) — 最后错误: {err_str}")
                            # 配额耗尽前先保存已下载的数据
                            if stock_dfs:
                                combined = pd.concat(stock_dfs, ignore_index=True)
                                self._save_stock_parquet(rq_code, combined, data_dir)
                            self._running = False
                            return
                        continue  # 切换成功,重试本段
                    else:
                        log.error(f"    {rq_code} {freq_label}[{seg_start}→{seg_end}]失败: {err_str}")
                        failed += 1

            # 该股所有段下载完成，一次性合并保存
            if stock_dfs:
                combined = pd.concat(stock_dfs, ignore_index=True)
                self._save_stock_parquet(rq_code, combined, data_dir)

            # 进度日志(每100只)
            if stock_num % 100 == 0 or stock_num == total_stocks:
                log.info(f"  {freq_label}补齐进度: {stock_num}/{total_stocks}只 "
                         f"(完成{done}段 失败{failed} 拒绝{rejected})")
                self._switch_key_if_needed()

            time.sleep(0.05)

        log.info(f"  ✓ {freq_label}补齐完成: 成功{done} 失败{failed} 拒绝{rejected}")

    def _save_stock_parquet(self, rq_code, new_df, data_dir):
        """保存单只股票数据到parquet(合并去重)
        
        注意: 米筐日线index名是'date'，1m/5m是'datetime'，统一为'datetime'
        """
        # 统一时间列名为datetime
        if 'date' in new_df.columns and 'datetime' not in new_df.columns:
            new_df = new_df.rename(columns={'date': 'datetime'})
        
        f = self._find_stock_file(rq_code, data_dir)
        if f and f.exists():
            existing = pd.read_parquet(f)
            # 已有文件也可能用date，统一
            if 'date' in existing.columns and 'datetime' not in existing.columns:
                existing = existing.rename(columns={'date': 'datetime'})
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=['datetime', 'order_book_id'], keep='last')
            combined = combined.sort_values('datetime').reset_index(drop=True)
            combined.to_parquet(f, index=False)
        else:
            out_name = rq_code.replace('.XSHG', '_XSHG').replace('.XSHE', '_XSHE').replace('.INDX', '_INDX') + '.parquet'
            out_file = data_dir / out_name
            new_df = new_df.sort_values('datetime').reset_index(drop=True)
            new_df.to_parquet(out_file, index=False)

    # ============================================================
    # 开盘5分钟tick下载
    # ============================================================
    def _download_open_tick(self, rq, stock_list):
        """下载全市场A股开盘5分钟tick数据(09:25-09:35)

        保存到: OPEN_TICK_DIR/{code}/{YYYY-MM-DD}.parquet
        字段: 米筐TICK_FIELDS(含5档盘口)
        """
        log.info(f"  下载开盘5分钟tick: {len(stock_list)}只A股")
        OPEN_TICK_DIR.mkdir(parents=True, exist_ok=True)

        today_str = date.today().isoformat()
        batch_size = 10  # tick数据量大，小批次
        done = 0
        failed = 0

        for i in range(0, len(stock_list), batch_size):
            batch = stock_list[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(stock_list) - 1) // batch_size + 1

            for rq_code in batch:
                try:
                    df = rq.get_price(
                        rq_code,
                        start_date=today_str,
                        end_date=today_str,
                        frequency='tick',
                        fields=TICK_FIELDS,
                        expect_df=True
                    )

                    if df is None or df.empty:
                        continue

                    df = df.reset_index()
                    # 过滤开盘5分钟: 09:25 ~ 09:35
                    if 'datetime' in df.columns:
                        df['datetime'] = pd.to_datetime(df['datetime'])
                        mask = (df['datetime'].dt.strftime('%H:%M') >= '09:25') & \
                               (df['datetime'].dt.strftime('%H:%M') <= '09:35')
                        df = df[mask]

                    if df.empty:
                        continue

                    # 保存到 OPEN_TICK_DIR/{code}/{date}.parquet
                    code_prefix = rq_code.replace('.XSHG', '_XSHG').replace('.XSHE', '_XSHE').replace('.INDX', '_INDX')
                    out_dir = OPEN_TICK_DIR / code_prefix
                    out_dir.mkdir(parents=True, exist_ok=True)
                    out_file = out_dir / f'{today_str}.parquet'
                    df.to_parquet(out_file, index=False)
                    done += 1

                except Exception as e:
                    err_str = str(e)[:100]
                    # 配额耗尽/连接数超限 → 尝试切换Key
                    if 'quota' in err_str.lower() or 'limit' in err_str.lower() or 'login machine' in err_str.lower():
                        if not self._switch_key_if_needed(0.5):
                            log.error(f"  ⚠ Key1配额耗尽，停止开盘tick下载 — 最后错误: {err_str}")
                            return
                        continue  # 切换成功,重试
                    else:
                        log.error(f"    {rq_code} 开盘tick失败: {err_str}")
                        failed += 1

            if batch_num % 100 == 0 or batch_num == total_batches:
                log.info(f"  开盘tick进度: {batch_num}/{total_batches}批 "
                         f"(完成{done} 失败{failed})")
                self._switch_key_if_needed()

            time.sleep(0.1)

        log.info(f"  ✓ 开盘tick下载完成: 成功{done} 失败{failed}")

    @property
    def progress(self):
        return dict(self._progress)


# ============================================================
# 统一行情中心
# ============================================================
class MarketHubV2:
    """统一行情中心 v2

    1. ZMQ订阅herzqt实时tick → 保存parquet + ZMQ分发 + K线聚合
    2. 米筐日更1m/5m/daily
    3. FastAPI Web接口
    """

    def __init__(self, zmq_host='quote.herzqt.com', zmq_port=19002,
                 snap_port=19800, bar_port=19801, query_port=19802,
                 api_port=19803):
        self.zmq_host = zmq_host
        self.zmq_port = zmq_port
        self.snap_port = snap_port
        self.bar_port = bar_port
        self.query_port = query_port
        self.api_port = api_port

        self._ctx = zmq.Context()
        self._remote_sub = None
        self._snap_pub = None
        self._bar_pub = None
        self._query_rep = None

        self._running = False
        self._start_time = None

        # 核心组件
        self.aggregator = BarAggregator(max_bars=500)
        self.tick_saver = TickSaver()
        self.rq_updater = RqDataUpdater()

        # 快照缓存
        self._snapshots = {}
        self._snap_lock = threading.Lock()

        # 收数据/处理数据 分离: Queue
        import queue
        self._raw_queue = queue.Queue(maxsize=200000)  # 收数据→处理数据的桥梁
        self._save_queue = queue.Queue(maxsize=200000)  # 处理数据→保存的桥梁

        # 统计
        self._msg_count = 0
        self._snap_count = 0
        self._bar_count = 0
        self._codes_seen = set()
        self._index_codes = set()
        self._last_report = 0

        # ZMQ自动重连: 连续N分钟0消息则重建socket
        self._reconnect_threshold = 3  # 连续3分钟0消息触发重连
        self._zero_msg_minutes = 0
        self._last_msg_minute = 0
        self._need_reconnect = False  # 重连标志(线程安全：主线程设，recv线程执行)

    def connect(self):
        """连接远程ZMQ + 启动本地PUB/REP"""
        self._remote_sub = self._ctx.socket(zmq.SUB)
        self._remote_sub.setsockopt_string(zmq.SUBSCRIBE, '')
        self._remote_sub.setsockopt(zmq.RCVTIMEO, 5000)
        self._remote_sub.setsockopt(zmq.RCVHWM, 500000)
        self._remote_sub.setsockopt(zmq.LINGER, 0)
        self._remote_sub.setsockopt(zmq.RECONNECT_IVL, 1000)     # 首次重连1秒
        self._remote_sub.setsockopt(zmq.RECONNECT_IVL_MAX, 5000) # 最大重连间隔5秒
        addr = f'tcp://{self.zmq_host}:{self.zmq_port}'
        self._remote_sub.connect(addr)
        log.info(f"✅ 远程ZMQ连接: {addr}")

        self._snap_pub = self._ctx.socket(zmq.PUB)
        self._snap_pub.setsockopt(zmq.SNDHWM, 500000)
        self._snap_pub.bind(f'tcp://*:{self.snap_port}')
        log.info(f"✅ 快照PUB: tcp://*:{self.snap_port}")

        self._bar_pub = self._ctx.socket(zmq.PUB)
        self._bar_pub.setsockopt(zmq.SNDHWM, 100000)
        self._bar_pub.bind(f'tcp://*:{self.bar_port}')
        log.info(f"✅ K线PUB: tcp://*:{self.bar_port}")

        self._query_rep = self._ctx.socket(zmq.REP)
        self._query_rep.setsockopt(zmq.RCVTIMEO, 1000)
        self._query_rep.bind(f'tcp://*:{self.query_port}')
        log.info(f"✅ 查询REP: tcp://*:{self.query_port}")

        return True

    def _reconnect_remote(self):
        """ZMQ SUB自动重连: 关闭旧socket, 重建新socket"""
        try:
            if self._remote_sub is not None:
                self._remote_sub.close()
        except Exception:
            pass
        self._remote_sub = self._ctx.socket(zmq.SUB)
        self._remote_sub.setsockopt_string(zmq.SUBSCRIBE, '')
        self._remote_sub.setsockopt(zmq.RCVTIMEO, 5000)
        self._remote_sub.setsockopt(zmq.RCVHWM, 500000)
        self._remote_sub.setsockopt(zmq.LINGER, 0)
        # ZMQ重连策略: 快速重试
        self._remote_sub.setsockopt(zmq.RECONNECT_IVL, 1000)     # 首次1秒
        self._remote_sub.setsockopt(zmq.RECONNECT_IVL_MAX, 5000) # 最大5秒
        addr = f'tcp://{self.zmq_host}:{self.zmq_port}'
        self._remote_sub.connect(addr)
        self._zero_msg_minutes = 0
        log.warning(f"🔄 ZMQ SUB自动重连: {addr}")

    def _check_zmq_health(self):
        """检测ZMQ数据健康度: 盘中连续N分钟0消息则标记需要重连(排除午休)
        注意: 只设标志，不直接操作ZMQ socket(非线程安全)，由_recv_thread执行重连
        """
        now_min = int(time.time()) // 60
        from datetime import datetime
        now = datetime.now()
        t = now.hour * 100 + now.minute
        # 只在交易时段检测: 9:15~11:30, 13:00~15:05
        in_market = (915 <= t <= 1130) or (1300 <= t <= 1505)
        if not in_market:
            self._zero_msg_minutes = 0
            self._need_reconnect = False
            return

        if self._msg_count > self._last_msg_minute:
            # 有新消息
            self._last_msg_minute = self._msg_count
            self._zero_msg_minutes = 0
        else:
            self._zero_msg_minutes += 1
            if self._zero_msg_minutes >= self._reconnect_threshold:
                log.error(f"⚠️ ZMQ数据中断{self._zero_msg_minutes}分钟, 标记需要重连!")
                self._need_reconnect = True

    def _process_remote_msg(self, frames):
        """处理远程ZMQ消息"""
        if len(frames) < 2:
            return

        topic = frames[0].decode('utf-8', errors='replace')
        try:
            payload = json.loads(frames[1].decode('utf-8', errors='replace'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return

        self._msg_count += 1

        if topic == 'market':
            self._process_snapshot(payload)

    def _process_snapshot(self, data):
        """处理快照 → 缓存 + tick保存 + K线聚合 + ZMQ分发"""
        bar = data.get('bar', data)

        sid = str(bar.get('SecurityID', '')).strip()
        if not sid:
            return
        lp = bar.get('LastPrice', 0)
        pc = bar.get('PreClosePrice', 0)
        if pc <= 0:
            return

        # 代码标准化
        is_idx = sid in KNOWN_INDICES
        if is_idx:
            code = sid + '.IDX'
            self._index_codes.add(code)
        elif sid.startswith('6'):
            code = sid + '.SH'
        elif sid.startswith(('0', '3', '4', '8')):
            code = sid + '.SZ'
        else:
            return

        self._codes_seen.add(code)

        ut = bar.get('UpdateTime', '')
        ms = bar.get('UpdateMillisec', 0)
        if not ut:
            return

        # 构造标准化快照(含5档盘口)
        snap = {
            'code': code,
            'last_price': float(lp),
            'open': float(bar.get('OpenPrice', 0)),
            'high': float(bar.get('HighestPrice', bar.get('HighPrice', 0))),
            'low': float(bar.get('LowestPrice', 0)),
            'pre_close': float(pc),
            'upper_limit': float(bar.get('UpperLimitPrice', 0)),
            'lower_limit': float(bar.get('LowerLimitPrice', 0)),
            'volume': int(bar.get('Volume', 0)),
            'turnover': float(bar.get('Turnover', 0)),
            # R6修复: 5档买卖盘(原仅1档, S6假托盘/假压制/露头就打需要)
            'ask1': float(bar.get('AskPrice1', 0)), 'ask_vol1': int(bar.get('AskVolume1', 0)),
            'ask2': float(bar.get('AskPrice2', 0)), 'ask_vol2': int(bar.get('AskVolume2', 0)),
            'ask3': float(bar.get('AskPrice3', 0)), 'ask_vol3': int(bar.get('AskVolume3', 0)),
            'ask4': float(bar.get('AskPrice4', 0)), 'ask_vol4': int(bar.get('AskVolume4', 0)),
            'ask5': float(bar.get('AskPrice5', 0)), 'ask_vol5': int(bar.get('AskVolume5', 0)),
            'bid1': float(bar.get('BidPrice1', 0)), 'bid_vol1': int(bar.get('BidVolume1', 0)),
            'bid2': float(bar.get('BidPrice2', 0)), 'bid_vol2': int(bar.get('BidVolume2', 0)),
            'bid3': float(bar.get('BidPrice3', 0)), 'bid_vol3': int(bar.get('BidVolume3', 0)),
            'bid4': float(bar.get('BidPrice4', 0)), 'bid_vol4': int(bar.get('BidVolume4', 0)),
            'bid5': float(bar.get('BidPrice5', 0)), 'bid_vol5': int(bar.get('BidVolume5', 0)),
            'update_time': ut,
            'update_ms': int(ms),
            'is_index': is_idx,
            'trading_day': str(bar.get('TradingDay', '')),
        }

        # 缓存
        with self._snap_lock:
            self._snapshots[code] = snap

        # ★ Tick保存
        tick_record = {
            'timestamp': f"{snap['trading_day']} {ut}.{ms:03d}",
            'code': code,
            'last_price': snap['last_price'],
            'volume': snap['volume'],
            'turnover': snap['turnover'],
            'open': snap['open'],
            'high': snap['high'],
            'low': snap['low'],
            'pre_close': snap['pre_close'],
            'ask1': snap['ask1'], 'ask_vol1': snap['ask_vol1'],
            'bid1': snap['bid1'], 'bid_vol1': snap['bid_vol1'],
        }
        self._save_queue.put(tick_record, block=False)  # 放入保存队列，不阻塞处理线程

        # ★ K线聚合
        vol = snap['volume']
        to = snap['turnover']
        completed_1m, completed_5m = self.aggregator.update(
            code=code, price=snap['last_price'],
            volume=vol, turnover=to, timestamp_str=ut,
            pre_close=snap['pre_close'], open_price=snap['open'],
            high_price=snap['high'], low_price=snap['low'],
            ask1=snap['ask1'], bid1=snap['bid1'],
        )

        # ★ ZMQ分发快照(兼容旧客户端)
        snap_topic = f"snap.{code}"
        snap_json = json.dumps(snap, ensure_ascii=False).encode('utf-8')
        try:
            self._snap_pub.send_multipart([snap_topic.encode('utf-8'), snap_json])
        except Exception:
            pass
        self._snap_count += 1

        # ★ ZMQ分发1分钟K线
        if completed_1m is not None:
            bar_topic = f"bar.1m.{code}"
            bar_json = json.dumps(completed_1m, ensure_ascii=False).encode('utf-8')
            try:
                self._bar_pub.send_multipart([bar_topic.encode('utf-8'), bar_json])
            except Exception:
                pass
            self._bar_count += 1

    def _handle_query(self):
        """处理ZMQ查询(兼容旧客户端)"""
        try:
            if not self._query_rep.poll(100):  # 查询线程独立运行，100ms等待合理
                return
            msg = self._query_rep.recv_string()
            req = json.loads(msg)
        except Exception:
            return

        cmd = req.get('cmd', '')
        if cmd == 'snapshot':
            code = req.get('code', '')
            with self._snap_lock:
                data = self._snapshots.get(code)
            self._query_rep.send_json({'ok': True, 'data': data} if data
                                      else {'ok': False, 'error': f'No snapshot for {code}'})
        elif cmd == 'bars':
            code = req.get('code', '')
            count = req.get('count', 120)
            bars = self.aggregator.get_1m_bars(code, count)
            self._query_rep.send_json({'ok': True, 'data': bars})
        elif cmd == 'stats':
            elapsed = time.time() - self._start_time if self._start_time else 0
            with self._snap_lock:
                snap_count = len(self._snapshots)
            ts = self.tick_saver.stats()
            self._query_rep.send_json({'ok': True, 'data': {
                'total_msgs': self._msg_count, 'snap_published': self._snap_count,
                'bars_published': self._bar_count, 'codes_seen': len(self._codes_seen),
                'index_count': len(self._index_codes), 'snapshot_cached': snap_count,
                'tick_saved': ts['tick_count'], 'tick_files': ts['file_count'],
                'elapsed_sec': round(elapsed, 1),
                'rate_per_sec': round(self._msg_count / elapsed, 1) if elapsed > 0 else 0,
            }})
        elif cmd == 'tick':
            code = req.get('code', '')
            day = req.get('date', None)
            limit = req.get('limit', 1000)
            ticks = self.tick_saver.get_tick_data(code, day, limit=limit)
            self._query_rep.send_json({'ok': True, 'data': ticks})
        else:
            self._query_rep.send_json({'ok': False, 'error': f'Unknown cmd: {cmd}'})

    def _report_stats(self):
        """定期报告统计"""
        now = time.time()
        if now - self._last_report < 60:
            return
        self._last_report = now
        elapsed = now - self._start_time if self._start_time else 1
        rate = self._msg_count / elapsed
        with self._snap_lock:
            snap_cached = len(self._snapshots)
        ts = self.tick_saver.stats()
        log.info(f"📊 统计: 总{self._msg_count}条 | {rate:.0f}条/秒 | "
                 f"快照{self._snap_count} | K线{self._bar_count} | "
                 f"缓存{snap_cached}只 | tick保存{ts['tick_count']}条")

    def _check_scheduled_tasks(self):
        """检查定时任务
        16:30 — 下载当日最新数据 (米筐16:00左右更新完毕)
        23:00-23:59 — 消耗剩余配额，逐日向前补齐历史缺失数据 (24:00配额重置)
        """
        now = datetime.now()
        # 任务1: 16:30-16:59 下载当日数据
        # 窗口扩大到30分钟: 防止16:30时_running=True(启动时任务还在跑)导致任务永久丢失
        # _last_today_download去重保证同一天只触发一次
        if now.hour == 16 and now.minute >= 30:
            should_download = (
                self.rq_updater._last_today_download is None or
                self.rq_updater._last_today_download.date() != now.date() or
                # 关键修复: 如果上次下载在16:30之前(启动时下载的)，需要重新下载完整数据
                self.rq_updater._last_today_download.hour < 16
            )
            # 检查_running防止重入
            if should_download and not self.rq_updater._running:
                log.info("⏰ 16:30 触发米筐当日下载(全市场A股)")
                # 注意: 不再强制重置_running=False，避免多线程重入
                self.rq_updater._last_today_download = None
                threading.Thread(target=self.rq_updater.download_today_update,
                                 daemon=True).start()
        # 任务2: 23:00历史补齐 — 已禁用(用户要求停止)
        # if now.hour == 23 and now.minute <= 5:
        #     if self.rq_updater._last_backfill is None or \
        #        self.rq_updater._last_backfill.date() != now.date():
        #         if not self.rq_updater._running:
        #             log.info("⏰ 23:00 触发米筐历史补齐(消耗剩余配额)")
        #             threading.Thread(target=self.rq_updater.backfill_history,
        #                              daemon=True).start()
        #         else:
        #             log.warning("⏰ 23:00 历史补齐被跳过: 16:30下载任务仍在运行")

    def _recv_thread(self):
        """线程1: 纯收数据 — 只做ZMQ接收，放入队列
        注意: ZMQ Socket不是线程安全的，必须在本线程内创建和使用
        """
        log.info("✅ 收数据线程启动")
        # 在本线程内创建ZMQ SUB socket
        addr = f'tcp://{self.zmq_host}:{self.zmq_port}'
        recv_sub = self._ctx.socket(zmq.SUB)
        recv_sub.setsockopt_string(zmq.SUBSCRIBE, '')
        recv_sub.setsockopt(zmq.RCVTIMEO, 5000)
        recv_sub.setsockopt(zmq.RCVHWM, 500000)
        recv_sub.setsockopt(zmq.LINGER, 0)
        recv_sub.connect(addr)
        log.info(f"✅ 收数据线程ZMQ连接: {addr}")

        while self._running:
            try:
                # 批量接收: 非阻塞消费ZMQ缓冲区
                batch = 0
                for _ in range(500):
                    try:
                        frames = recv_sub.recv_multipart(zmq.NOBLOCK)
                        self._raw_queue.put(frames, block=False)
                        batch += 1
                    except zmq.Again:
                        break
                    except queue.Full:
                        break
                if batch == 0:
                    # 无数据时短暂等待
                    time.sleep(0.001)
            except zmq.ZMQError as e:
                log.error(f"收数据ZMQ错误: {e}")
                time.sleep(2)
            except Exception as e:
                log.error(f"收数据异常: {e}")
                time.sleep(1)

    def _process_thread(self):
        """线程2: 处理数据 — 解析+聚合+分发+入保存队列"""
        log.info("✅ 处理数据线程启动")
        while self._running:
            try:
                # 批量从队列取数据
                frames = self._raw_queue.get(timeout=0.1)
                self._process_remote_msg(frames)
                # 顺便消费队列中的剩余数据
                for _ in range(200):
                    try:
                        frames = self._raw_queue.get_nowait()
                        self._process_remote_msg(frames)
                    except queue.Empty:
                        break
            except queue.Empty:
                pass
            except Exception as e:
                log.error(f"处理数据异常: {e}")

    def _save_thread(self):
        """线程3: 保存数据 — tick写parquet，不阻塞收/处理"""
        log.info("✅ 保存数据线程启动")
        while self._running:
            try:
                tick_record = self._save_queue.get(timeout=1)
                self.tick_saver.add(tick_record['code'], tick_record)
                # 顺便消费队列中的剩余数据
                for _ in range(500):
                    try:
                        tick_record = self._save_queue.get_nowait()
                        self.tick_saver.add(tick_record['code'], tick_record)
                    except queue.Empty:
                        break
            except queue.Empty:
                pass
            except Exception as e:
                log.error(f"保存数据异常: {e}")

    def _query_thread(self):
        """线程4: ZMQ查询响应 — 独立线程，不阻塞其他逻辑"""
        log.info("✅ 查询响应线程启动")
        while self._running:
            try:
                self._handle_query()
                time.sleep(0.001)  # 1ms间隔
            except Exception as e:
                log.error(f"查询响应异常: {e}")
                time.sleep(0.1)

    def run(self):
        """主循环 — 四线程架构: 收数据 / 处理数据 / 保存数据 / 查询响应"""
        self._running = True
        self._start_time = time.time()
        self._last_report = time.time()

        log.info("=" * 60)
        log.info("统一行情中心 v2 (Market Hub)")
        log.info(f"远程ZMQ: tcp://{self.zmq_host}:{self.zmq_port}")
        log.info(f"快照PUB: tcp://*:{self.snap_port}")
        log.info(f"K线PUB:  tcp://*:{self.bar_port}")
        log.info(f"查询REP: tcp://*:{self.query_port}")
        log.info(f"FastAPI: http://0.0.0.0:{self.api_port}")
        log.info("架构: 收数据线程 → 处理数据线程 → 保存数据线程 + 查询响应线程")
        log.info("=" * 60)

        # 启动FastAPI(在子线程中)
        api_thread = threading.Thread(target=self._run_api, daemon=True)
        api_thread.start()
        log.info(f"✅ FastAPI已启动: http://0.0.0.0:{self.api_port}")

        # 启动四个工作线程
        t_recv = threading.Thread(target=self._recv_thread, daemon=True)
        t_proc = threading.Thread(target=self._process_thread, daemon=True)
        t_save = threading.Thread(target=self._save_thread, daemon=True)
        t_query = threading.Thread(target=self._query_thread, daemon=True)
        t_recv.start()
        t_proc.start()
        t_save.start()
        t_query.start()

        while self._running:
            try:
                # 主线程只做: 统计 + 健康检查 + 定时任务
                self._report_stats()
                self._check_zmq_health()
                self._check_scheduled_tasks()
                time.sleep(1)  # 主线程1秒一次循环，避免_zero_msg_minutes疯涨

            except KeyboardInterrupt:
                break
            except Exception as e:
                log.error(f"未知错误: {e}")
                time.sleep(1)

        self._running = False
        # 最终flush
        self.tick_saver.flush()
        log.info(f"行情中心退出，共处理 {self._msg_count} 条消息")

    def _run_api(self):
        """在子线程中运行FastAPI"""
        import uvicorn
        from fastapi import FastAPI, Query
        from fastapi.responses import JSONResponse

        app = FastAPI(title="Market Hub API", version="2.0")

        @app.get("/api/snapshot/{code}")
        def api_snapshot(code: str):
            """查询最新快照"""
            with self._snap_lock:
                data = self._snapshots.get(code)
            if data:
                return {"ok": True, "data": data}
            return {"ok": False, "error": f"No snapshot for {code}"}

        @app.get("/api/snapshots")
        def api_all_snapshots(index_only: bool = Query(False)):
            """查询全部快照"""
            with self._snap_lock:
                if index_only:
                    data = {c: s for c, s in self._snapshots.items() if s.get('is_index')}
                else:
                    data = dict(self._snapshots)
            return {"ok": True, "data": data, "count": len(data)}

        @app.get("/api/tick/{code}")
        def api_tick(code: str, date_: str = Query(None, alias="date"),
                     start: str = Query(None), end: str = Query(None),
                     limit: int = Query(1000)):
            """查询tick数据"""
            ticks = self.tick_saver.get_tick_data(code, date_, start, end, limit)
            return {"ok": True, "data": ticks, "count": len(ticks)}

        @app.get("/api/bars/1m/{code}")
        def api_1m_bars(code: str, count: int = Query(120)):
            """查询实时1分钟K线(从ZMQ聚合)"""
            bars = self.aggregator.get_1m_bars(code, count)
            return {"ok": True, "data": bars, "count": len(bars)}

        @app.get("/api/bars/5m/{code}")
        def api_5m_bars(code: str, count: int = Query(48)):
            """查询实时5分钟K线(从ZMQ聚合)"""
            bars = self.aggregator.get_5m_bars(code, count)
            return {"ok": True, "data": bars, "count": len(bars)}

        @app.get("/api/history/1m/{code}")
        def api_history_1m(code: str, count: int = Query(240)):
            """查询历史1分钟K线(从parquet文件, 米筐数据)"""
            rq_code = code_to_rqdata(code)
            prefix = rq_code.replace('.XSHG', '_XSHG').replace('.XSHE', '_XSHE').replace('.INDX', '_INDX')

            # 先查指数目录 (指数文件名: 000001_XSHG_1m.parquet)
            import glob as glob_mod
            candidates = glob_mod.glob(str(INDEX_1M_DIR / f'{prefix}_1m.parquet'))
            if not candidates:
                # 个股目录 (个股文件名: 600740_XSHG_3696.parquet)
                candidates = glob_mod.glob(str(MINUTE_DIR / f'{prefix}*.parquet'))
            if not candidates:
                return {"ok": False, "error": f"No 1m data for {code}"}

            f = candidates[0]

            try:
                df = pd.read_parquet(f)
                df = df.tail(count)
                records = df.to_dict('records')
                # 转换NaN
                for r in records:
                    for k, v in r.items():
                        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                            r[k] = None
                        elif hasattr(v, 'item'):
                            r[k] = v.item()
                return {"ok": True, "data": records, "count": len(records)}
            except Exception as e:
                return {"ok": False, "error": str(e)}

        @app.get("/api/history/5m/{code}")
        def api_history_5m(code: str, count: int = Query(48)):
            """查询历史5分钟K线(从parquet文件, 米筐数据)"""
            rq_code = code_to_rqdata(code)
            prefix = rq_code.replace('.XSHG', '_XSHG').replace('.XSHE', '_XSHE').replace('.INDX', '_INDX')
            import glob as glob_mod
            candidates = glob_mod.glob(str(FIVE_MIN_DIR / f'{prefix}*.parquet'))
            if not candidates:
                return {"ok": False, "error": f"No 5m data for {code}"}
            f = candidates[0]
            try:
                df = pd.read_parquet(f)
                df = df.tail(count)
                records = df.to_dict('records')
                for r in records:
                    for k, v in r.items():
                        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                            r[k] = None
                        elif hasattr(v, 'item'):
                            r[k] = v.item()
                return {"ok": True, "data": records, "count": len(records)}
            except Exception as e:
                return {"ok": False, "error": str(e)}

        @app.get("/api/history/daily/{code}")
        def api_history_daily(code: str, count: int = Query(120)):
            """查询历史日线(从parquet文件, 米筐数据)"""
            rq_code = code_to_rqdata(code)
            prefix = rq_code.replace('.XSHG', '_XSHG').replace('.XSHE', '_XSHE').replace('.INDX', '_INDX')
            import glob as glob_mod
            candidates = glob_mod.glob(str(DAILY_DIR / f'{prefix}*.parquet'))
            if not candidates:
                return {"ok": False, "error": f"No daily data for {code}"}
            f = candidates[0]
            try:
                df = pd.read_parquet(f)
                df = df.tail(count)
                records = df.to_dict('records')
                for r in records:
                    for k, v in r.items():
                        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                            r[k] = None
                        elif hasattr(v, 'item'):
                            r[k] = v.item()
                return {"ok": True, "data": records, "count": len(records)}
            except Exception as e:
                return {"ok": False, "error": str(e)}

        @app.get("/api/stats")
        def api_stats():
            """查询行情中心统计"""
            elapsed = time.time() - self._start_time if self._start_time else 0
            with self._snap_lock:
                snap_count = len(self._snapshots)
            ts = self.tick_saver.stats()
            rq_today = self.rq_updater._last_today_download.isoformat() if self.rq_updater._last_today_download else None
            rq_backfill = self.rq_updater._last_backfill.isoformat() if self.rq_updater._last_backfill else None
            return {
                "ok": True,
                "data": {
                    "total_msgs": self._msg_count,
                    "snap_published": self._snap_count,
                    "bars_published": self._bar_count,
                    "codes_seen": len(self._codes_seen),
                    "index_count": len(self._index_codes),
                    "snapshot_cached": snap_count,
                    "tick_saved": ts['tick_count'],
                    "tick_files": ts['file_count'],
                    "rq_last_today_download": rq_today,
                    "rq_last_backfill": rq_backfill,
                    "elapsed_sec": round(elapsed, 1),
                    "rate_per_sec": round(self._msg_count / elapsed, 1) if elapsed > 0 else 0,
                }
            }

        @app.post("/api/rqdata/update")
        def api_rqdata_update(mode: str = Query('today', description="today=当日下载, backfill=历史补齐")):
            """手动触发米筐数据下载"""
            if mode == 'backfill':
                threading.Thread(target=self.rq_updater.backfill_history, daemon=True).start()
                return {"ok": True, "message": "米筐历史补齐任务已启动"}
            else:
                threading.Thread(target=self.rq_updater.download_today_update, daemon=True).start()
                return {"ok": True, "message": "米筐当日下载任务已启动"}

        uvicorn.run(app, host="0.0.0.0", port=self.api_port, log_level="warning")

    def stop(self):
        self._running = False

    def cleanup(self):
        self.tick_saver.flush()
        for sock in (self._remote_sub, self._snap_pub, self._bar_pub, self._query_rep):
            if sock:
                sock.close()
        self._ctx.term()


# ============================================================
# 行情中心客户端 (兼容旧版 + 新增tick/5m查询)
# ============================================================
class MarketHubClient:
    """行情中心客户端 — 策略程序通过此类获取行情"""

    def __init__(self, hub_host='127.0.0.1',
                 snap_port=19800, bar_port=19801, query_port=19802):
        self.hub_host = hub_host
        self.snap_port = snap_port
        self.bar_port = bar_port
        self.query_port = query_port
        self._ctx = zmq.Context()
        self._snap_sub = None
        self._bar_sub = None
        self._query_req = None

    def connect(self):
        self._snap_sub = self._ctx.socket(zmq.SUB)
        self._snap_sub.setsockopt(zmq.RCVTIMEO, 1000)
        self._snap_sub.setsockopt(zmq.RCVHWM, 100000)
        self._snap_sub.setsockopt(zmq.LINGER, 0)
        self._snap_sub.connect(f'tcp://{self.hub_host}:{self.snap_port}')

        self._bar_sub = self._ctx.socket(zmq.SUB)
        self._bar_sub.setsockopt(zmq.RCVTIMEO, 1000)
        self._bar_sub.setsockopt(zmq.RCVHWM, 100000)
        self._bar_sub.setsockopt(zmq.LINGER, 0)
        self._bar_sub.connect(f'tcp://{self.hub_host}:{self.bar_port}')

        self._query_req = self._ctx.socket(zmq.REQ)
        self._query_req.setsockopt(zmq.RCVTIMEO, 3000)
        self._query_req.connect(f'tcp://{self.hub_host}:{self.query_port}')
        return True

    def subscribe_snap(self, code=''):
        topic = f"snap.{code}" if code else "snap."
        self._snap_sub.setsockopt_string(zmq.SUBSCRIBE, topic)

    def subscribe_bar(self, interval='1m', code=''):
        topic = f"bar.{interval}.{code}" if code else f"bar.{interval}."
        self._bar_sub.setsockopt_string(zmq.SUBSCRIBE, topic)

    def recv_snap(self, timeout_ms=1000):
        try:
            if not self._snap_sub.poll(timeout_ms):
                return None
            frames = self._snap_sub.recv_multipart()
            if len(frames) >= 2:
                return json.loads(frames[1].decode('utf-8'))
        except Exception:
            pass
        return None

    def recv_bar(self, timeout_ms=1000):
        try:
            if not self._bar_sub.poll(timeout_ms):
                return None
            frames = self._bar_sub.recv_multipart()
            if len(frames) >= 2:
                return json.loads(frames[1].decode('utf-8'))
        except Exception:
            pass
        return None

    def query_snapshot(self, code):
        try:
            self._query_req.send_json({'cmd': 'snapshot', 'code': code})
            resp = self._query_req.recv_json()
            return resp.get('data') if resp.get('ok') else None
        except Exception:
            return None

    def query_bars(self, code, count=120):
        try:
            self._query_req.send_json({'cmd': 'bars', 'code': code, 'count': count})
            resp = self._query_req.recv_json()
            return resp.get('data') if resp.get('ok') else None
        except Exception:
            return None

    def query_tick(self, code, day=None, limit=1000):
        """查询tick数据(新增)"""
        try:
            self._query_req.send_json({'cmd': 'tick', 'code': code, 'date': day, 'limit': limit})
            resp = self._query_req.recv_json()
            return resp.get('data') if resp.get('ok') else None
        except Exception:
            return None

    def query_stats(self):
        try:
            self._query_req.send_json({'cmd': 'stats'})
            resp = self._query_req.recv_json()
            return resp.get('data') if resp.get('ok') else None
        except Exception:
            return None

    def close(self):
        for sock in (self._snap_sub, self._bar_sub, self._query_req):
            if sock:
                sock.close()
        self._ctx.term()


# ============================================================
# 主程序
# ============================================================
def main():
    parser = argparse.ArgumentParser(description='统一行情中心 v2 (Market Hub)')
    parser.add_argument('--zmq-host', default='quote.herzqt.com')
    parser.add_argument('--zmq-port', type=int, default=19002)
    parser.add_argument('--snap-port', type=int, default=19800)
    parser.add_argument('--bar-port', type=int, default=19801)
    parser.add_argument('--query-port', type=int, default=19802)
    parser.add_argument('--api-port', type=int, default=19803)
    args = parser.parse_args()

    hub = MarketHubV2(
        zmq_host=args.zmq_host, zmq_port=args.zmq_port,
        snap_port=args.snap_port, bar_port=args.bar_port,
        query_port=args.query_port, api_port=args.api_port,
    )

    def signal_handler(sig, frame):
        log.info(f"收到信号 {sig}，停止...")
        hub.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    hub.connect()
    try:
        hub.run()
    finally:
        hub.cleanup()


if __name__ == '__main__':
    main()
