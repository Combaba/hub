#!/usr/bin/env python3
"""
QMT Mini 行情+交易中心 (QMT Hub)
=================================
miniQMT版统一行情中心 + 交易网关，接口兼容 market_hub_v2.py + huaxin_trade_gateway.py。

策略程序无需修改，仅切换连接目标即可从herzqt+华鑫 切换到 miniQMT。

架构:
  xtdata.get_full_tick() ──poll──▸ QMT Hub
                                    ├─ ZMQ PUB :19800  [snap]  快照分发
                                    ├─ ZMQ PUB :19801  [bar]   1m K线分发
                                    ├─ ZMQ REP :19802  [query] ZMQ行情查询
                                    ├─ FastAPI :19803  [http]  Web API
                                    ├─ ZMQ REP :19850  [trade] 交易网关
                                    └─ 文件保存 tick→parquet

行情兼容 (与 market_hub_v2 完全一致):
  - ZMQ PUB snap/bar 主题格式: snap.{code}, bar.1m.{code}
  - ZMQ REP 查询协议: {"cmd":"snapshot","code":"600000.SH"}
  - FastAPI 端点: /api/snapshot/{code}, /api/snapshots, /api/bars/1m/{code}, ...
  - 快照字段: code, last_price, open, high, low, pre_close, volume, turnover,
              ask1~ask5, ask_vol1~ask_vol5, bid1~bid5, bid_vol1~bid_vol5,
              update_time, update_ms, is_index, trading_day

交易兼容 (与 huaxin_trade_gateway 完全一致):
  - ZMQ REP :19850 协议: {"action":"buy","stock":"600000.SH","shares":100,"price":10.5,"order_type":"LIMIT"}
  - action: ping | buy | sell | query_account | query_position | query_orders | query_trades
  - 响应: {"ok":true/false, ...}

QMT tick字段映射:
  QMT get_full_tick() → MarketHub snapshot:
    lastPrice    → last_price
    open         → open
    high         → high
    low          → low
    lastClose    → pre_close
    volume       → volume
    amount       → turnover
    askPrice[0]  → ask1, ..., askPrice[4] → ask5
    bidPrice[0]  → bid1, ..., bidPrice[4] → bid5
    askVol[0]    → ask_vol1, ..., askVol[4] → ask_vol5
    bidVol[0]    → bid_vol1, ..., bidVol[4] → bid_vol5

运行:
  python qmt_hub.py --qmt-path "D:/QMTgj/userdata_mini" --account 8884972726
  python qmt_hub.py --qmt-path "D:/QMTgj/userdata_mini" --account 8884972726 --dry-run
"""

import sys
import os
import json
import time
import argparse
import logging
import signal
import threading
import asyncio
from datetime import datetime, date, timedelta
from pathlib import Path
from collections import defaultdict, deque

import zmq
import numpy as np
import pandas as pd

from market_hub_v2 import (
    TickSaver, BarAggregator, normalize_code, is_index, code_to_rqdata,
    TICK_DIR, OPEN_TICK_DIR, MINUTE_DIR, FIVE_MIN_DIR, DAILY_DIR,
    INDEX_1M_DIR, DATA_ROOT, KNOWN_INDICES,
)

try:
    from xtquant import xtdata, xtconstant
    from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
    from xtquant.xttype import StockAccount
    QMT_AVAILABLE = True
except ImportError:
    QMT_AVAILABLE = False

LOG_FILE = DATA_ROOT / 'qmt_hub.log'

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.FileHandler(str(LOG_FILE), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger('qmt_hub')

POLL_INTERVAL = 3
QMT_CODE_LIST_REFRESH = 300


def qmt_tick_to_snapshot(qmt_code, tick):
    """QMT get_full_tick() dict → MarketHub-compatible snapshot dict"""
    if not tick:
        return None
    last_price = tick.get('lastPrice', 0)
    pre_close = tick.get('lastClose', 0)
    if not pre_close:
        return None

    code6 = qmt_code.split('.')[0] if '.' in qmt_code else qmt_code
    code = normalize_code(code6)
    if code is None:
        return None
    is_idx = is_index(code)

    ask_prices = tick.get('askPrice', [0] * 5)
    bid_prices = tick.get('bidPrice', [0] * 5)
    ask_vols = tick.get('askVol', [0] * 5)
    bid_vols = tick.get('bidVol', [0] * 5)
    while len(ask_prices) < 5:
        ask_prices.append(0)
    while len(bid_prices) < 5:
        bid_prices.append(0)
    while len(ask_vols) < 5:
        ask_vols.append(0)
    while len(bid_vols) < 5:
        bid_vols.append(0)

    update_time = tick.get('timetag', '')
    update_ms = 0
    if isinstance(update_time, int):
        update_ms = update_time % 1000
        update_time = f"{update_time // 10000000:02d}:{(update_time // 100000) % 100:02d}:{(update_time // 1000) % 100:02d}"
    if not update_time:
        update_time = datetime.now().strftime('%H:%M:%S')

    trading_day = str(tick.get('tradingDate', tick.get('tradingDay', '')))
    if not trading_day:
        trading_day = str(date.today())

    snap = {
        'code': code,
        'last_price': float(last_price),
        'open': float(tick.get('open', 0)),
        'high': float(tick.get('high', 0)),
        'low': float(tick.get('low', 0)),
        'pre_close': float(pre_close),
        'upper_limit': float(tick.get('upperLimit', 0)),
        'lower_limit': float(tick.get('lowerLimit', 0)),
        'volume': int(tick.get('volume', 0)),
        'turnover': float(tick.get('amount', 0)),
        'ask1': float(ask_prices[0]), 'ask_vol1': int(ask_vols[0]),
        'ask2': float(ask_prices[1]), 'ask_vol2': int(ask_vols[1]),
        'ask3': float(ask_prices[2]), 'ask_vol3': int(ask_vols[2]),
        'ask4': float(ask_prices[3]), 'ask_vol4': int(ask_vols[3]),
        'ask5': float(ask_prices[4]), 'ask_vol5': int(ask_vols[4]),
        'bid1': float(bid_prices[0]), 'bid_vol1': int(bid_vols[0]),
        'bid2': float(bid_prices[1]), 'bid_vol2': int(bid_vols[1]),
        'bid3': float(bid_prices[2]), 'bid_vol3': int(bid_vols[2]),
        'bid4': float(bid_prices[3]), 'bid_vol4': int(bid_vols[3]),
        'bid5': float(bid_prices[4]), 'bid_vol5': int(bid_vols[4]),
        'update_time': update_time,
        'update_ms': update_ms,
        'is_index': is_idx,
        'trading_day': trading_day,
    }
    return snap


class QMTTraderCallback(XtQuantTraderCallback):
    def __init__(self, hub):
        super().__init__()
        self.hub = hub

    def on_connected(self):
        log.info("QMT trader connected")
        self.hub._qmt_logged_in = True

    def on_disconnected(self):
        log.warning("QMT trader DISCONNECTED")
        self.hub._qmt_logged_in = False

    def on_stock_order(self, order):
        log.info(f"[ORDER] {order.stock_code} type={order.order_type} "
                 f"vol={order.order_volume} price={order.price} "
                 f"status={order.order_status}")

    def on_stock_trade(self, trade):
        log.info(f"[TRADE] {trade.stock_code} "
                 f"vol={trade.traded_volume}@{trade.traded_price}")

    def on_stock_position(self, position):
        pass

    def on_stock_asset(self, asset):
        pass

    def on_order_error(self, order_error):
        log.error(f"[ORDER_ERROR] id={order_error.order_id} "
                  f"msg={order_error.error_msg}")


class QMTHub:
    """QMT Mini 行情+交易中心

    行情: xtdata.get_full_tick() → 快照缓存 + ZMQ PUB + K线聚合 + tick保存
    交易: XtQuantTrader → ZMQ REP :19850 (与huaxin_trade_gateway同协议)
    """

    def __init__(self, qmt_path='', account_id='', session_id=20260724,
                 snap_port=19800, bar_port=19801, query_port=19802,
                 api_port=19803, trade_port=19850, dry_run=False):
        self.qmt_path = qmt_path
        self.account_id = account_id
        self.session_id = session_id
        self.dry_run = dry_run

        self.snap_port = snap_port
        self.bar_port = bar_port
        self.query_port = query_port
        self.api_port = api_port
        self.trade_port = trade_port

        self._ctx = zmq.Context()
        self._snap_pub = None
        self._bar_pub = None
        self._query_rep = None
        self._trade_rep = None

        self._running = False
        self._start_time = None

        self.aggregator = BarAggregator(max_bars=500)
        self.tick_saver = TickSaver()

        self._snapshots = {}
        self._snap_lock = threading.Lock()

        self._all_codes = []
        self._last_code_refresh = 0

        self._msg_count = 0
        self._snap_count = 0
        self._bar_count = 0
        self._codes_seen = set()
        self._last_report = 0

        self.xt_trader = None
        self.account = None
        self._qmt_logged_in = False
        self._trade_lock = threading.Lock()

    def _init_qmt_data(self):
        """Initialize xtdata (no login required, just need miniQMT client running)"""
        if not QMT_AVAILABLE:
            log.error("xtquant not installed, cannot use QMT data")
            return False
        try:
            sectors = xtdata.get_stock_list_in_sector('沪深A股')
            if sectors:
                self._all_codes = sectors
                log.info(f"QMT A股列表: {len(sectors)}只")
            else:
                self._all_codes = []
                log.warning("QMT A股列表为空")
        except Exception as e:
            log.error(f"QMT get_stock_list_in_sector failed: {e}")
            self._all_codes = []
        return True

    def _init_qmt_trader(self):
        """Initialize XtQuantTrader for trading"""
        if not QMT_AVAILABLE:
            log.error("xtquant not installed, cannot trade")
            return False
        if self.dry_run:
            log.info("DRY-RUN mode: trading disabled")
            return True
        if not self.qmt_path or not self.account_id:
            log.warning("QMT path or account not set, trading0 trading disabled")
            return True

        try:
            self.xt_trader = XtQuantTrader(self.qmt_path, self.session_id)
            self.xt_trader.register_callback(QMTTraderCallback(self))
            self.xt_trader.start()
            result = self.xt_trader.connect()
            if result != 0:
                log.error(f"QMT trader connect failed: {result}")
                return False
            self.account = StockAccount(self.account_id)
            sub_result = self.xt_trader.subscribe(self.account)
            if sub_result != 0:
                log.error(f"QMT account subscribe failed: {sub_result}")
                return False
            log.info(f"QMT trader connected, account={self.account_id}")
            return True
        except Exception as e:
            log.error(f"QMT trader init error: {e}")
            return False

    def _refresh_code_list(self):
        """Refresh A股代码列表 periodically"""
        now = time.time()
        if now - self._last_code_refresh < QMT_CODE_LIST_REFRESH:
            return
        self._last_code_refresh = now
        try:
            sectors = xtdata.get_stock_list_in_sector('沪深A股')
            if sectors:
                self._all_codes = sectors
                log.info(f"A股列表刷新: {len(sectors)}只")
        except Exception as e:
            log.warning(f"A股列表刷新失败: {e}")

    def _poll_ticks(self):
        """Poll xtdata.get_full_tick() for all codes, convert to snapshots"""
        if not self._all_codes:
            return

        self._refresh_code_list()

        try:
            ticks = xtdata.get_full_tick(self._all_codes)
        except Exception as e:
            log.error(f"get_full_tick error: {e}")
            return

        if not ticks:
            return

        for qmt_code, tick_data in ticks.items():
            if not tick_data:
                continue
            snap = qmt_tick_to_snapshot(qmt_code, tick_data)
            if snap is None:
                continue

            code = snap['code']
            self._msg_count += 1
            self._codes_seen.add(code)

            with self._snap_lock:
                self._snapshots[code] = snap

            tick_record = {
                'timestamp': f"{snap['trading_day']} {snap['update_time']}.{snap['update_ms']:03d}",
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
            self.tick_saver.add(code, tick_record)

            ut = snap['update_time']
            completed_1m, _ = self.aggregator.update(
                code=code, price=snap['last_price'],
                volume=snap['volume'], turnover=snap['turnover'],
                timestamp_str=ut,
                pre_close=snap['pre_close'], open_price=snap['open'],
                high_price=snap['high'], low_price=snap['low'],
                ask1=snap['ask1'], bid1=snap['bid1'],
            )

            snap_topic = f"snap.{code}"
            snap_json = json.dumps(snap, ensure_ascii=False).encode('utf-8')
            try:
                self._snap_pub.send_multipart([snap_topic.encode('utf-8'), snap_json])
            except Exception:
                pass
            self._snap_count += 1

            if completed_1m is not None:
                bar_topic = f"bar.1m.{code}"
                bar_json = json.dumps(completed_1m, ensure_ascii=False).encode('utf-8')
                try:
                    self._bar_pub.send_multipart([bar_topic.encode('utf-8'), bar_json])
                except Exception:
                    pass
                self._bar_count += 1

    def _handle_query(self):
        """Handle ZMQ行情查询 (same protocol as market_hub_v2)"""
        try:
            if not self._query_rep.poll(100):
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
                'snapshot_cached': snap_count,
                'tick_saved': ts['tick_count'], 'tick_files': ts['file_count'],
                'elapsed_sec': round(elapsed, 1),
                'rate_per_sec': round(self._msg_count / elapsed, 1) if elapsed > 0 else 0,
                'source': 'qmt_mini',
            }})
        elif cmd == 'tick':
            code = req.get('code', '')
            day = req.get('date', None)
            limit = req.get('limit', 1000)
            ticks = self.tick_saver.get_tick_data(code, day, limit=limit)
            self._query_rep.send_json({'ok': True, 'data': ticks})
        else:
            self._query_rep.send_json({'ok': False, 'error': f'Unknown cmd: {cmd}'})

    def _handle_trade_request(self):
        """Handle ZMQ交易请求 (same protocol as huaxin_trade_gateway)"""
        try:
            if not self._trade_rep.poll(100):
                return
            msg = self._trade_rep.recv_string()
            req = json.loads(msg)
        except Exception:
            return

        resp = self._process_trade_action(req)
        self._trade_rep.send_string(json.dumps(resp, ensure_ascii=False))

        action = req.get('action', '?')
        if action != 'ping':
            log.info(f"ZMQ交易请求: {action} {req.get('stock', '')} → ok={resp.get('ok')}")

    def _process_trade_action(self, req):
        """Process trade action (compatible with huaxin_trade_gateway)"""
        action = req.get('action', '')

        if action == 'ping':
            return {
                'ok': True,
                'status': 'alive',
                'logged_in': self._qmt_logged_in,
                'disconnected': False,
                'consecutive_fail': 0,
                'source': 'qmt_mini',
            }

        if action == 'query_account':
            return self._qmt_query_account()
        if action == 'query_position':
            return self._qmt_query_position(stock=req.get('stock', ''))
        if action == 'query_orders':
            return self._qmt_query_orders(stock=req.get('stock', ''))
        if action == 'query_trades':
            return self._qmt_query_trades(stock=req.get('stock', ''))

        if action not in ('buy', 'sell'):
            return {'ok': False, 'error': f'unknown action: {action}'}

        stock = req.get('stock', '')
        if not stock:
            return {'ok': False, 'error': 'stock is required'}

        if not self._qmt_logged_in or self.xt_trader is None:
            return {'ok': False, 'error': 'QMT trader not connected'}

        shares = int(req.get('shares', 0))
        price = float(req.get('price', 0))
        order_type = req.get('order_type', 'LIMIT')
        reason = req.get('reason', '')

        if order_type not in ('LIMIT', 'MARKET'):
            return {'ok': False, 'error': f'invalid order_type: {order_type}'}

        shares = (shares // 100) * 100
        if shares <= 0:
            return {'ok': False, 'error': 'shares must be >= 100'}

        return self._qmt_send_order(stock, shares, price, action, order_type, reason)

    def _qmt_query_account(self):
        """Query account (compatible with huaxin query_account)"""
        if self.dry_run or self.xt_trader is None or self.account is None:
            return {'ok': True, 'data': [{'account_id': self.account_id or 'dry_run',
                                          'pre_deposit': 0, 'useful_money': 0,
                                          'frozen_cash': 0, 'frozen_commission': 0,
                                          'commission': 0, 'deposit': 0, 'withdraw': 0}]}
        try:
            asset = self.xt_trader.query_stock_asset(self.account)
            if asset:
                return {'ok': True, 'data': [{
                    'account_id': self.account_id,
                    'pre_deposit': getattr(asset, 'total_asset', 0),
                    'useful_money': getattr(asset, 'cash', 0),
                    'frozen_cash': getattr(asset, 'frozen_cash', 0),
                    'frozen_commission': 0,
                    'commission': getattr(asset, 'commission', 0),
                    'deposit': getattr(asset, 'total_asset', 0),
                    'withdraw': 0,
                }]}
        except Exception as e:
            log.error(f"query_account error: {e}")
        return {'ok': False, 'error': 'query_account failed'}

    def _qmt_query_position(self, stock=''):
        """Query positions (compatible with huaxin query_position)"""
        if self.dry_run or self.xt_trader is None or self.account is None:
            return {'ok': True, 'data': []}
        try:
            positions = self.xt_trader.query_stock_positions(self.account)
            result = []
            if positions:
                for pos in positions:
                    vol = getattr(pos, 'volume', 0)
                    if vol <= 0:
                        continue
                    code = pos.stock_code
                    code6 = code.split('.')[0] if '.' in code else code
                    exchange = 'SSE' if code6.startswith('6') else 'SZSE'
                    if stock and code != stock and code6 != stock:
                        continue
                    result.append({
                        'exchange': exchange,
                        'security_id': code6,
                        'security_name': '',
                        'current_position': vol,
                        'available_position': getattr(pos, 'can_use_volume', 0),
                        'history_pos': 0,
                        'today_bs_pos': 0,
                        'history_pos_price': getattr(pos, 'open_price', 0),
                        'total_pos_cost': 0,
                        'close_profit': 0,
                        'today_commission': 0,
                        'today_total_buy_amount': 0,
                        'today_total_sell_amount': 0,
                    })
            return {'ok': True, 'data': result}
        except Exception as e:
            log.error(f"query_position error: {e}")
        return {'ok': False, 'error': 'query_position failed'}

    def _qmt_query_orders(self, stock=''):
        """Query orders (compatible with huaxin query_orders)"""
        if self.dry_run or self.xt_trader is None or self.account is None:
            return {'ok': True, 'data': []}
        try:
            orders = self.xt_trader.query_stock_orders(self.account)
            result = []
            if orders:
                dm = {xtconstant.STOCK_BUY: '买入', xtconstant.STOCK_SELL: '卖出'}
                sm = {
                    xtconstant.ORDER_UNREPORTED: '已报',
                    xtconstant.ORDER_REPORTED: '已报',
                    xtconstant.ORDER_PART_SUCC: '部成',
                    xtconstant.ORDER_SUCCEEDED: '全成',
                    xtconstant.ORDER_CANCELED: '全撤',
                    xtconstant.ORDER_JUNK: '拒单',
                }
                for o in orders:
                    if stock and o.stock_code != stock:
                        continue
                    code6 = o.stock_code.split('.')[0] if '.' in o.stock_code else o.stock_code
                    exchange = 'SSE' if code6.startswith('6') else 'SZSE'
                    result.append({
                        'exchange': exchange,
                        'security_id': code6,
                        'direction': dm.get(o.order_type, str(o.order_type)),
                        'order_status': sm.get(o.order_status, str(o.order_status)),
                        'limit_price': getattr(o, 'price', 0),
                        'volume_original': getattr(o, 'order_volume', 0),
                        'volume_traded': getattr(o, 'filled_volume', 0),
                        'volume_canceled': getattr(o, 'order_volume', 0) - getattr(o, 'filled_volume', 0),
                        'order_ref': getattr(o, 'order_id', 0),
                        'order_sys_id': str(getattr(o, 'order_id', 0)),
                        'insert_time': '',
                        'trading_day': str(date.today()),
                        'status_msg': '',
                    })
            return {'ok': True, 'data': result}
        except Exception as e:
            log.error(f"query_orders error: {e}")
        return {'ok': False, 'error': 'query_orders failed'}

    def _qmt_query_trades(self, stock=''):
        """Query trades (compatible with huaxin query_trades)"""
        if self.dry_run or self.xt_trader is None or self.account is None:
            return {'ok': True, 'data': []}
        try:
            trades = self.xt_trader.query_stock_trades(self.account)
            result = []
            if trades:
                dm = {xtconstant.STOCK_BUY: '买入', xtconstant.STOCK_SELL: '卖出'}
                for t in trades:
                    if stock and t.stock_code != stock:
                        continue
                    code6 = t.stock_code.split('.')[0] if '.' in t.stock_code else t.stock_code
                    exchange = 'SSE' if code6.startswith('6') else 'SZSE'
                    result.append({
                        'exchange': exchange,
                        'security_id': code6,
                        'trade_id': str(getattr(t, 'traded_id', 0)),
                        'direction': dm.get(getattr(t, 'order_type', 0), ''),
                        'price': getattr(t, 'traded_price', 0),
                        'volume': getattr(t, 'traded_volume', 0),
                        'trade_date': str(date.today()),
                        'trade_time': '',
                        'trading_day': str(date.today()),
                        'order_ref': getattr(t, 'order_id', 0),
                    })
            return {'ok': True, 'data': result}
        except Exception as e:
            log.error(f"query_trades error: {e}")
        return {'ok': False, 'error': 'query_trades failed'}

    def _qmt_send_order(self, stock, shares, price, action, order_type, reason):
        """Send buy/sell order via QMT"""
        if self.xt_trader is None or self.account is None:
            return {'ok': False, 'error': 'QMT trader not initialized'}

        qmt_code = stock
        if action == 'buy':
            order_type_const = xtconstant.STOCK_BUY
            action_label = '买入'
        else:
            order_type_const = xtconstant.STOCK_SELL
            action_label = '卖出'

        if order_type == 'MARKET':
            price_type = xtconstant.LATEST_PRICE
            limit_price = 0
        else:
            price_type = xtconstant.FIX_PRICE
            limit_price = price

        try:
            order_id = self.xt_trader.order_stock(
                self.account, qmt_code, order_type_const,
                shares, price_type, limit_price,
                'QMTHub', reason[:40]
            )
            if order_id > 0:
                log.info(f"[{action_label}] {stock} vol={shares} price={limit_price:.2f} id={order_id}")
                return {'ok': True, 'order_ref': order_id, 'status': 'pending', 'trades': []}
            else:
                log.error(f"[{action_label} FAILED] {stock} vol={shares} price={limit_price:.2f}")
                return {'ok': False, 'error': f'order_stock returned {order_id}'}
        except Exception as e:
            log.error(f"order error: {e}")
            return {'ok': False, 'error': str(e)}

    def _report_stats(self):
        now = time.time()
        if now - self._last_report < 60:
            return
        self._last_report = now
        elapsed = max(now - self._start_time, 1) if self._start_time else 1
        rate = self._msg_count / elapsed
        with self._snap_lock:
            snap_cached = len(self._snapshots)
        ts = self.tick_saver.stats()
        log.info(f"统计: 总{self._msg_count}条 | {rate:.0f}条/秒 | "
                 f"快照{self._snap_count} | K线{self._bar_count} | "
                 f"缓存{snap_cached}只 | tick{ts['tick_count']}条 | "
                 f"源=qmt_mini")

    def connect(self):
        """Initialize ZMQ sockets"""
        self._snap_pub = self._ctx.socket(zmq.PUB)
        self._snap_pub.setsockopt(zmq.SNDHWM, 500000)
        self._snap_pub.bind(f'tcp://*:{self.snap_port}')
        log.info(f"快照PUB: tcp://*:{self.snap_port}")

        self._bar_pub = self._ctx.socket(zmq.PUB)
        self._bar_pub.setsockopt(zmq.SNDHWM, 100000)
        self._bar_pub.bind(f'tcp://*:{self.bar_port}')
        log.info(f"K线PUB: tcp://*:{self.bar_port}")

        self._query_rep = self._ctx.socket(zmq.REP)
        self._query_rep.setsockopt(zmq.RCVTIMEO, 1000)
        self._query_rep.bind(f'tcp://*:{self.query_port}')
        log.info(f"行情查询REP: tcp://*:{self.query_port}")

        self._trade_rep = self._ctx.socket(zmq.REP)
        self._trade_rep.setsockopt(zmq.RCVTIMEO, 1000)
        self._trade_rep.bind(f'tcp://*:{self.trade_port}')
        log.info(f"交易网关REP: tcp://*:{self.trade_port}")

        return True

    def run(self):
        """Main loop"""
        self._running = True
        self._start_time = time.time()
        self._last_report = time.time()

        log.info("=" * 60)
        log.info("QMT Mini 行情+交易中心 (QMT Hub)")
        log.info(f"快照PUB: tcp://*:{self.snap_port}")
        log.info(f"K线PUB:  tcp://*:{self.bar_port}")
        log.info(f"行情查询REP: tcp://*:{self.query_port}")
        log.info(f"FastAPI: http://0.0.0.0:{self.api_port}")
        log.info(f"交易网关REP: tcp://*:{self.trade_port}")
        log.info(f"QMT路径: {self.qmt_path or '(未设置)'}")
        log.info(f"交易账户: {self.account_id or '(未设置)'}")
        log.info(f"Dry-run: {self.dry_run}")
        log.info("=" * 60)

        if not self._init_qmt_data():
            log.error("QMT行情初始化失败")
            return

        self._init_qmt_trader()

        api_thread = threading.Thread(target=self._run_api, daemon=True)
        api_thread.start()
        log.info(f"FastAPI已启动: http://0.0.0.0:{self.api_port}")

        while self._running:
            try:
                self._poll_ticks()
                self._handle_query()
                self._handle_trade_request()
                self._report_stats()
            except KeyboardInterrupt:
                break
            except Exception as e:
                log.error(f"主循环错误: {e}")
                time.sleep(1)

        self._running = False
        self.tick_saver.flush()
        log.info(f"QMT Hub退出，共处理 {self._msg_count} 条消息")

    def _run_api(self):
        """FastAPI in sub-thread (same endpoints as market_hub_v2)"""
        import uvicorn
        from fastapi import FastAPI, Query

        app = FastAPI(title="QMT Hub API", version="1.0")

        @app.get("/api/snapshot/{code}")
        def api_snapshot(code: str):
            with self._snap_lock:
                data = self._snapshots.get(code)
            if data:
                return {"ok": True, "data": data}
            return {"ok": False, "error": f"No snapshot for {code}"}

        @app.get("/api/snapshots")
        def api_all_snapshots(index_only: bool = Query(False)):
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
            ticks = self.tick_saver.get_tick_data(code, date_, start, end, limit)
            return {"ok": True, "data": ticks, "count": len(ticks)}

        @app.get("/api/bars/1m/{code}")
        def api_1m_bars(code: str, count: int = Query(120)):
            bars = self.aggregator.get_1m_bars(code, count)
            return {"ok": True, "data": bars, "count": len(bars)}

        @app.get("/api/bars/5m/{code}")
        def api_5m_bars(code: str, count: int = Query(48)):
            bars = self.aggregator.get_5m_bars(code, count)
            return {"ok": True, "data": bars, "count": len(bars)}

        @app.get("/api/history/1m/{code}")
        def api_history_1m(code: str, count: int = Query(120)):
            return {"ok": False, "error": "History data via QMT not supported, use rqdata"}

        @app.get("/api/history/5m/{code}")
        def api_history_5m(code: str, count: int = Query(48)):
            return {"ok": False, "error": "History data via QMT not supported, use rqdata"}

        @app.get("/api/history/daily/{code}")
        def api_history_daily(code: str, count: int = Query(60)):
            return {"ok": False, "error": "History data via QMT not supported, use rqdata"}

        @app.get("/api/stats")
        def api_stats():
            elapsed = time.time() - self._start_time if self._start_time else 0
            with self._snap_lock:
                snap_count = len(self._snapshots)
            ts = self.tick_saver.stats()
            return {"ok": True, "data": {
                "total_msgs": self._msg_count,
                "snap_published": self._snap_count,
                "bars_published": self._bar_count,
                "codes_seen": len(self._codes_seen),
                "snapshot_cached": snap_count,
                "tick_saved": ts['tick_count'],
                "tick_files": ts['file_count'],
                "elapsed_sec": round(elapsed, 1),
                "rate_per_sec": round(self._msg_count / elapsed, 1) if elapsed > 0 else 0,
                "source": "qmt_mini",
            }}

        @app.post("/api/rqdata/update")
        def api_rqdata_update(mode: str = Query('today')):
            return {"ok": False, "error": "QMT Hub does not support rqdata update"}

        uvicorn.run(app, host="0.0.0.0", port=self.api_port, log_level="warning")

    def cleanup(self):
        self.tick_saver.flush()
        for sock in (self._snap_pub, self._bar_pub, self._query_rep, self._trade_rep):
            if sock:
                try:
                    sock.setsockopt(zmq.LINGER, 0)
                    sock.close()
                except Exception:
                    pass
        self._ctx.term()


class QMTMarketHubClient:
    """行情客户端 — 复用 market_hub_v2.MarketHubClient (接口完全兼容)"""

    def __init__(self, hub_host='127.0.0.1',
                 snap_port=19800, bar_port=19801, query_port=19802):
        from market_hub_v2 import MarketHubClient
        self._inner = MarketHubClient(hub_host, snap_port, bar_port, query_port)

    def connect(self):
        return self._inner.connect()

    def subscribe_snap(self, code=''):
        return self._inner.subscribe_snap(code)

    def subscribe_bar(self, interval='1m', code=''):
        return self._inner.subscribe_bar(interval, code)

    def recv_snap(self, timeout_ms=1000):
        return self._inner.recv_snap(timeout_ms)

    def recv_bar(self, timeout_ms=1000):
        return self._inner.recv_bar(timeout_ms)

    def query_snapshot(self, code):
        return self._inner.query_snapshot(code)

    def query_bars(self, code, count=120):
        return self._inner.query_bars(code, count)

    def query_tick(self, code, day=None, limit=1000):
        return self._inner.query_tick(code, day, limit)

    def query_stats(self):
        return self._inner.query_stats()

    def close(self):
        return self._inner.close()


class QMTTradeClient:
    """交易客户端 — 通过ZMQ REQ与QMT Hub交易网关通信 (与华鑫网关协议兼容)"""

    def __init__(self, hub_host='127.0.0.1', trade_port=19850):
        self.hub_host = hub_host
        self.trade_port = trade_port
        self._ctx = zmq.Context()
        self._sock = None

    def connect(self):
        self._sock = self._ctx.socket(zmq.REQ)
        self._sock.setsockopt(zmq.RCVTIMEO, 5000)
        self._sock.connect(f'tcp://{self.hub_host}:{self.trade_port}')
        return True

    def _request(self, req_dict):
        if self._sock is None:
            return {'ok': False, 'error': 'not connected'}
        try:
            self._sock.send_string(json.dumps(req_dict))
            resp = self._sock.recv_string()
            return json.loads(resp)
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    def ping(self):
        return self._request({'action': 'ping'})

    def buy(self, stock, shares, price, order_type='LIMIT', reason=''):
        return self._request({
            'action': 'buy', 'stock': stock, 'shares': shares,
            'price': price, 'order_type': order_type, 'reason': reason,
        })

    def sell(self, stock, shares, price, order_type='LIMIT', reason=''):
        return self._request({
            'action': 'sell', 'stock': stock, 'shares': shares,
            'price': price, 'order_type': order_type, 'reason': reason,
        })

    def query_account(self):
        return self._request({'action': 'query_account'})

    def query_position(self, stock=''):
        return self._request({'action': 'query_position', 'stock': stock})

    def query_orders(self, stock=''):
        return self._request({'action': 'query_orders', 'stock': stock})

    def query_trades(self, stock=''):
        return self._request({'action': 'query_trades', 'stock': stock})

    def close(self):
        if self._sock:
            self._sock.close()
        self._ctx.term()


def main():
    parser = argparse.ArgumentParser(description='QMT Mini 行情+交易中心')
    parser.add_argument('--qmt-path', type=str, default='',
                        help='miniQMT userdata_mini path')
    parser.add_argument('--account', type=str, default='',
                        help='QMT trading account ID')
    parser.add_argument('--session-id', type=int, default=20260724,
                        help='QMT session ID (unique per strategy)')
    parser.add_argument('--snap-port', type=int, default=19800)
    parser.add_argument('--bar-port', type=int, default=19801)
    parser.add_argument('--query-port', type=int, default=19802)
    parser.add_argument('--api-port', type=int, default=19803)
    parser.add_argument('--trade-port', type=int, default=19850)
    parser.add_argument('--dry-run', action='store_true',
                        help='Dry-run mode: no real trades')
    args = parser.parse_args()

    if not QMT_AVAILABLE:
        log.error("xtquant not installed. pip install xtquant")
        sys.exit(1)

    hub = QMTHub(
        qmt_path=args.qmt_path,
        account_id=args.account,
        session_id=args.session_id,
        snap_port=args.snap_port,
        bar_port=args.bar_port,
        query_port=args.query_port,
        api_port=args.api_port,
        trade_port=args.trade_port,
        dry_run=args.dry_run,
    )

    def signal_handler(sig, frame):
        log.info(f"收到信号 {sig}，停止...")
        hub._running = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    hub.connect()
    try:
        hub.run()
    finally:
        hub.cleanup()


if __name__ == '__main__':
    main()
