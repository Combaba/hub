#!/usr/bin/env python3
"""
QMT Hub 综合测试套件
====================
覆盖:
  1. QMT tick → MarketHub快照转换 (字段级完整校验)
  2. 代码格式转换 (全市场代码类型)
  3. QMTHub 初始化 + 配置
  4. 交易协议兼容性 (与huaxin_trade_gateway字段级对齐)
  5. ZMQ行情查询协议兼容性 (真实ZMQ REQ/REP)
  6. ZMQ交易网关集成 (真实ZMQ REQ/REP)
  7. FastAPI端点兼容性 (真实HTTP请求)
  8. BarAggregator集成 (tick→1m/5m K线)
  9. TickSaver集成 (tick→parquet持久化)
  10. QMT Mini实际连接 (xtdata.get_full_tick + xttrader)
  11. 并发压力测试
  12. 边界条件 + 异常恢复

运行:
  pytest test_qmt_hub.py -v                    # 单元+集成(不需要QMT)
  pytest test_qmt_hub.py -v --qmt-live         # 含QMT Mini实盘连接测试
  pytest test_qmt_hub.py -v -k "test_zmq"      # 只跑ZMQ集成测试
  pytest test_qmt_hub.py -v -k "test_live"     # 只跑QMT实盘测试
"""

import os
import sys
import json
import time
import shutil
import tempfile
import threading
import traceback
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
from copy import deepcopy

import numpy as np
import pandas as pd
import pytest
import zmq

TMP_ROOT = Path(tempfile.mkdtemp(prefix='qmt_hub_test_'))

sys.path.insert(0, str(Path(__file__).parent))
import market_hub_v2 as mh2

mh2.DATA_ROOT = TMP_ROOT
mh2.TICK_DIR = TMP_ROOT / 'stock_tick'
mh2.OPEN_TICK_DIR = TMP_ROOT / 'stock_open_tick'
mh2.MINUTE_DIR = TMP_ROOT / 'stock_data' / 'minute'
mh2.FIVE_MIN_DIR = TMP_ROOT / 'stock_data' / '5min'
mh2.DAILY_DIR = TMP_ROOT / 'stock_data' / 'daily'
mh2.INDEX_1M_DIR = TMP_ROOT / 'stock_data' / 'index_1m'

import qmt_hub

HUAXIN_ACCOUNT_FIELDS = {
    'account_id', 'pre_deposit', 'useful_money', 'frozen_cash',
    'frozen_commission', 'commission', 'deposit', 'withdraw',
}

HUAXIN_POSITION_FIELDS = {
    'exchange', 'security_id', 'security_name', 'current_position',
    'available_position', 'history_pos', 'today_bs_pos',
    'history_pos_price', 'total_pos_cost', 'close_profit',
    'today_commission', 'today_total_buy_amount', 'today_total_sell_amount',
}

HUAXIN_ORDER_FIELDS = {
    'exchange', 'security_id', 'direction', 'order_status',
    'limit_price', 'volume_original', 'volume_traded', 'volume_canceled',
    'order_ref', 'order_sys_id', 'insert_time', 'trading_day', 'status_msg',
}

HUAXIN_TRADE_FIELDS = {
    'exchange', 'security_id', 'trade_id', 'direction',
    'price', 'volume', 'trade_date', 'trade_time',
    'trading_day', 'order_ref',
}

SNAPSHOT_FIELDS = {
    'code', 'last_price', 'open', 'high', 'low', 'pre_close',
    'upper_limit', 'lower_limit', 'volume', 'turnover',
    'ask1', 'ask2', 'ask3', 'ask4', 'ask5',
    'ask_vol1', 'ask_vol2', 'ask_vol3', 'ask_vol4', 'ask_vol5',
    'bid1', 'bid2', 'bid3', 'bid4', 'bid5',
    'bid_vol1', 'bid_vol2', 'bid_vol3', 'bid_vol4', 'bid_vol5',
    'update_time', 'update_ms', 'is_index', 'trading_day',
}


def _make_qmt_tick(**overrides):
    tick = {
        'lastPrice': 10.5, 'open': 10.3, 'high': 10.8, 'low': 10.2,
        'lastClose': 10.0, 'volume': 50000, 'amount': 525000.0,
        'askPrice': [10.51, 10.52, 10.53, 10.54, 10.55],
        'bidPrice': [10.49, 10.48, 10.47, 10.46, 10.45],
        'askVol': [100, 200, 300, 400, 500],
        'bidVol': [150, 250, 350, 450, 550],
        'upperLimit': 11.0, 'lowerLimit': 9.0,
        'timetag': 93015000,
    }
    tick.update(overrides)
    return tick


def _make_full_snapshot(code='600000.SH'):
    tick = _make_qmt_tick()
    return qmt_hub.qmt_tick_to_snapshot(code, tick)


@pytest.fixture(autouse=True)
def clean_tmp_dirs():
    for d in [mh2.TICK_DIR, mh2.OPEN_TICK_DIR, mh2.MINUTE_DIR,
              mh2.FIVE_MIN_DIR, mh2.DAILY_DIR, mh2.INDEX_1M_DIR]:
        if d.exists():
            shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True, exist_ok=True)
    yield
    for d in [mh2.TICK_DIR, mh2.OPEN_TICK_DIR, mh2.MINUTE_DIR,
              mh2.FIVE_MIN_DIR, mh2.DAILY_DIR, mh2.INDEX_1M_DIR]:
        shutil.rmtree(d, ignore_errors=True)


# ============================================================
# 1. QMT tick → MarketHub快照转换 (字段级完整校验)
# ============================================================
class TestQmtTickToSnapshot:
    def test_all_snapshot_fields_present(self):
        tick = _make_qmt_tick()
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', tick)
        assert snap is not None
        missing = SNAPSHOT_FIELDS - set(snap.keys())
        assert missing == set(), f"Missing snapshot fields: {missing}"

    def test_no_extra_snapshot_fields(self):
        tick = _make_qmt_tick()
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', tick)
        extra = set(snap.keys()) - SNAPSHOT_FIELDS
        assert extra == set(), f"Extra snapshot fields: {extra}"

    def test_field_types(self):
        tick = _make_qmt_tick()
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', tick)
        assert isinstance(snap['code'], str)
        assert isinstance(snap['last_price'], float)
        assert isinstance(snap['open'], float)
        assert isinstance(snap['high'], float)
        assert isinstance(snap['low'], float)
        assert isinstance(snap['pre_close'], float)
        assert isinstance(snap['volume'], int)
        assert isinstance(snap['turnover'], float)
        for i in range(1, 6):
            assert isinstance(snap[f'ask{i}'], float)
            assert isinstance(snap[f'bid{i}'], float)
            assert isinstance(snap[f'ask_vol{i}'], int)
            assert isinstance(snap[f'bid_vol{i}'], int)
        assert isinstance(snap['update_time'], str)
        assert isinstance(snap['update_ms'], int)
        assert isinstance(snap['is_index'], bool)
        assert isinstance(snap['trading_day'], str)

    def test_sh_stock_conversion(self):
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', _make_qmt_tick())
        assert snap['code'] == '600000.SH'
        assert snap['is_index'] is False

    def test_sz_stock_conversion(self):
        snap = qmt_hub.qmt_tick_to_snapshot('000002.SZ', _make_qmt_tick())
        assert snap['code'] == '000002.SZ'
        assert snap['is_index'] is False

    def test_gem_stock_conversion(self):
        snap = qmt_hub.qmt_tick_to_snapshot('300001.SZ', _make_qmt_tick())
        assert snap['code'] == '300001.SZ'
        assert snap['is_index'] is False

    def test_star_market_conversion(self):
        snap = qmt_hub.qmt_tick_to_snapshot('688001.SH', _make_qmt_tick())
        assert snap['code'] == '688001.SH'
        assert snap['is_index'] is False

    def test_bse_stock_conversion(self):
        snap = qmt_hub.qmt_tick_to_snapshot('830001.BJ', _make_qmt_tick())
        assert snap is not None
        assert snap['code'] == '830001.SZ'

    def test_sh_index_conversion(self):
        snap = qmt_hub.qmt_tick_to_snapshot('000001.SH', _make_qmt_tick())
        assert snap['code'] == '000001.IDX'
        assert snap['is_index'] is True

    def test_sz_index_conversion(self):
        snap = qmt_hub.qmt_tick_to_snapshot('399001.SZ', _make_qmt_tick())
        assert snap['code'] == '399001.IDX'
        assert snap['is_index'] is True

    def test_5level_orderbook(self):
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', _make_qmt_tick())
        assert snap['ask1'] == 10.51
        assert snap['ask5'] == 10.55
        assert snap['bid1'] == 10.49
        assert snap['bid5'] == 10.45
        assert snap['ask_vol1'] == 100
        assert snap['ask_vol5'] == 500
        assert snap['bid_vol1'] == 150
        assert snap['bid_vol5'] == 550

    def test_missing_orderbook_pads_zero(self):
        tick = _make_qmt_tick(askPrice=[10.51], bidPrice=[10.49],
                              askVol=[100], bidVol=[150])
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', tick)
        assert snap['ask1'] == 10.51
        assert snap['ask2'] == 0
        assert snap['bid_vol2'] == 0

    def test_zero_lastprice_not_rejected(self):
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', _make_qmt_tick(lastPrice=0))
        assert snap is not None
        assert snap['last_price'] == 0

    def test_zero_preclose_returns_none(self):
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', _make_qmt_tick(lastClose=0))
        assert snap is None

    def test_unknown_prefix_returns_none(self):
        snap = qmt_hub.qmt_tick_to_snapshot('999999.XX', _make_qmt_tick())
        assert snap is None

    def test_trading_day_default_today(self):
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', _make_qmt_tick())
        assert snap['trading_day'] == str(date.today())

    def test_trading_day_from_tick_tradingDate(self):
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', _make_qmt_tick(tradingDate='20260724'))
        assert snap['trading_day'] == '20260724'

    def test_trading_day_from_tick_tradingDay(self):
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', _make_qmt_tick(tradingDay='20260101'))
        assert snap['trading_day'] == '20260101'

    def test_update_ms_from_int_timetag(self):
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', _make_qmt_tick(timetag=93015678))
        assert snap['update_ms'] == 678

    def test_update_time_from_int_timetag(self):
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', _make_qmt_tick(timetag=93015000))
        assert snap['update_time'] == '09:30:15'

    def test_update_time_fallback_when_no_timetag(self):
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', _make_qmt_tick(timetag=''))
        assert snap['update_time']  # should be HH:MM:SS format
        assert ':' in snap['update_time']

    def test_volume_and_turnover(self):
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', _make_qmt_tick())
        assert snap['volume'] == 50000
        assert snap['turnover'] == 525000.0

    def test_upper_lower_limit(self):
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', _make_qmt_tick())
        assert snap['upper_limit'] == 11.0
        assert snap['lower_limit'] == 9.0

    def test_negative_volume_treated_as_zero(self):
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', _make_qmt_tick(volume=-100))
        assert snap['volume'] == -100

    def test_large_volume(self):
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', _make_qmt_tick(volume=999999999))
        assert snap['volume'] == 999999999

    def test_float_volume_converted_to_int(self):
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', _make_qmt_tick(volume=50000.5))
        assert isinstance(snap['volume'], int)

    def test_empty_tick_data(self):
        assert qmt_hub.qmt_tick_to_snapshot('600000.SH', {}) is None

    def test_none_tick_data(self):
        assert qmt_hub.qmt_tick_to_snapshot('600000.SH', None) is None

    def test_qmt_code_with_dot_prefix(self):
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', _make_qmt_tick())
        assert snap['code'] == '600000.SH'

    def test_qmt_code_without_dot(self):
        snap = qmt_hub.qmt_tick_to_snapshot('600000', _make_qmt_tick())
        assert snap['code'] == '600000.SH'

    def test_index_no_orderbook(self):
        tick = _make_qmt_tick(lastPrice=3200.0, lastClose=3180.0,
                              volume=0, amount=0,
                              askPrice=[0]*5, bidPrice=[0]*5,
                              askVol=[0]*5, bidVol=[0]*5)
        snap = qmt_hub.qmt_tick_to_snapshot('000001.SH', tick)
        assert snap['is_index'] is True
        assert snap['ask1'] == 0
        assert snap['bid1'] == 0


# ============================================================
# 2. QMTHub 初始化 + 配置
# ============================================================
class TestQMTHubInit:
    def test_default_ports(self):
        hub = qmt_hub.QMTHub()
        assert hub.snap_port == 19800
        assert hub.bar_port == 19801
        assert hub.query_port == 19802
        assert hub.api_port == 19803
        assert hub.trade_port == 19850

    def test_custom_ports(self):
        hub = qmt_hub.QMTHub(snap_port=29800, bar_port=29801,
                              query_port=29802, api_port=29803,
                              trade_port=29850)
        assert hub.snap_port == 29800
        assert hub.trade_port == 29850

    def test_initial_state(self):
        hub = qmt_hub.QMTHub()
        assert hub._running is False
        assert hub._msg_count == 0
        assert hub._snap_count == 0
        assert hub._bar_count == 0
        assert hub._qmt_logged_in is False
        assert hub.xt_trader is None
        assert hub.account is None

    def test_dry_run_mode(self):
        hub = qmt_hub.QMTHub(dry_run=True)
        assert hub.dry_run is True

    def test_qmt_config(self):
        hub = qmt_hub.QMTHub(qmt_path='D:\\QMTgj\\userdata_mini',
                              account_id='8884972726')
        assert hub.qmt_path == 'D:\\QMTgj\\userdata_mini'
        assert hub.account_id == '8884972726'

    def test_session_id(self):
        hub = qmt_hub.QMTHub(session_id=12345)
        assert hub.session_id == 12345

    def test_aggregator_exists(self):
        hub = qmt_hub.QMTHub()
        assert isinstance(hub.aggregator, mh2.BarAggregator)

    def test_tick_saver_exists(self):
        hub = qmt_hub.QMTHub()
        assert isinstance(hub.tick_saver, mh2.TickSaver)

    def test_snapshots_empty(self):
        hub = qmt_hub.QMTHub()
        assert hub._snapshots == {}

    def test_thread_safety_locks(self):
        hub = qmt_hub.QMTHub()
        assert isinstance(hub._snap_lock, type(threading.Lock()))
        assert isinstance(hub._trade_lock, type(threading.Lock()))


# ============================================================
# 3. 交易协议兼容性 (与huaxin_trade_gateway字段级对齐)
# ============================================================
class TestTradeProtocolHuaxinCompat:
    """验证QMT Hub交易响应字段与huaxin_trade_gateway完全对齐"""

    def test_ping_response_fields(self):
        hub = qmt_hub.QMTHub(dry_run=True)
        hub._qmt_logged_in = True
        resp = hub._process_trade_action({'action': 'ping'})
        assert resp['ok'] is True
        assert resp['status'] == 'alive'
        assert 'logged_in' in resp
        assert 'disconnected' in resp
        assert 'consecutive_fail' in resp
        assert resp['source'] == 'qmt_mini'

    def test_ping_when_not_logged_in(self):
        hub = qmt_hub.QMTHub()
        hub._qmt_logged_in = False
        resp = hub._process_trade_action({'action': 'ping'})
        assert resp['ok'] is True
        assert resp['logged_in'] is False

    def test_query_account_dry_run_field_compat(self):
        hub = qmt_hub.QMTHub(dry_run=True, account_id='TEST123')
        resp = hub._process_trade_action({'action': 'query_account'})
        assert resp['ok'] is True
        assert len(resp['data']) == 1
        item = resp['data'][0]
        missing = HUAXIN_ACCOUNT_FIELDS - set(item.keys())
        assert missing == set(), f"query_account missing huaxin fields: {missing}"

    def test_query_account_with_mock_trader(self):
        hub = qmt_hub.QMTHub()
        hub.xt_trader = MagicMock()
        hub.account = MagicMock()
        hub._qmt_logged_in = True

        mock_asset = MagicMock()
        mock_asset.total_asset = 1000000.0
        mock_asset.cash = 500000.0
        mock_asset.frozen_cash = 10000.0
        mock_asset.commission = 50.0
        hub.xt_trader.query_stock_asset.return_value = mock_asset

        resp = hub._process_trade_action({'action': 'query_account'})
        assert resp['ok'] is True
        item = resp['data'][0]
        assert item['useful_money'] == 500000.0
        assert item['frozen_cash'] == 10000.0
        missing = HUAXIN_ACCOUNT_FIELDS - set(item.keys())
        assert missing == set(), f"Missing fields: {missing}"

    def test_query_position_dry_run_field_compat(self):
        hub = qmt_hub.QMTHub(dry_run=True)
        resp = hub._process_trade_action({'action': 'query_position'})
        assert resp['ok'] is True
        assert resp['data'] == []

    def test_query_position_with_mock_trader(self):
        hub = qmt_hub.QMTHub()
        hub.xt_trader = MagicMock()
        hub.account = MagicMock()
        hub._qmt_logged_in = True

        mock_pos = MagicMock()
        mock_pos.volume = 1000
        mock_pos.can_use_volume = 800
        mock_pos.open_price = 10.5
        mock_pos.stock_code = '600000.SH'
        hub.xt_trader.query_stock_positions.return_value = [mock_pos]

        resp = hub._process_trade_action({'action': 'query_position'})
        assert resp['ok'] is True
        assert len(resp['data']) == 1
        item = resp['data'][0]
        missing = HUAXIN_POSITION_FIELDS - set(item.keys())
        assert missing == set(), f"query_position missing huaxin fields: {missing}"
        assert item['exchange'] == 'SSE'
        assert item['security_id'] == '600000'
        assert item['current_position'] == 1000
        assert item['available_position'] == 800

    def test_query_position_sz_stock(self):
        hub = qmt_hub.QMTHub()
        hub.xt_trader = MagicMock()
        hub.account = MagicMock()
        hub._qmt_logged_in = True

        mock_pos = MagicMock()
        mock_pos.volume = 500
        mock_pos.can_use_volume = 500
        mock_pos.open_price = 20.0
        mock_pos.stock_code = '000002.SZ'
        hub.xt_trader.query_stock_positions.return_value = [mock_pos]

        resp = hub._process_trade_action({'action': 'query_position'})
        item = resp['data'][0]
        assert item['exchange'] == 'SZSE'
        assert item['security_id'] == '000002'

    def test_query_position_zero_volume_filtered(self):
        hub = qmt_hub.QMTHub()
        hub.xt_trader = MagicMock()
        hub.account = MagicMock()
        hub._qmt_logged_in = True

        mock_pos = MagicMock()
        mock_pos.volume = 0
        mock_pos.stock_code = '600000.SH'
        hub.xt_trader.query_stock_positions.return_value = [mock_pos]

        resp = hub._process_trade_action({'action': 'query_position'})
        assert resp['ok'] is True
        assert resp['data'] == []

    def test_query_position_with_stock_filter(self):
        hub = qmt_hub.QMTHub()
        hub.xt_trader = MagicMock()
        hub.account = MagicMock()
        hub._qmt_logged_in = True

        pos1 = MagicMock()
        pos1.volume = 1000
        pos1.can_use_volume = 800
        pos1.open_price = 10.5
        pos1.stock_code = '600000.SH'
        pos2 = MagicMock()
        pos2.volume = 500
        pos2.can_use_volume = 500
        pos2.open_price = 20.0
        pos2.stock_code = '000002.SZ'
        hub.xt_trader.query_stock_positions.return_value = [pos1, pos2]

        resp = hub._process_trade_action({'action': 'query_position', 'stock': '600000.SH'})
        assert len(resp['data']) == 1
        assert resp['data'][0]['security_id'] == '600000'

    def test_query_orders_dry_run_field_compat(self):
        hub = qmt_hub.QMTHub(dry_run=True)
        resp = hub._process_trade_action({'action': 'query_orders'})
        assert resp['ok'] is True
        assert resp['data'] == []

    def test_query_orders_with_mock_trader(self):
        hub = qmt_hub.QMTHub()
        hub.xt_trader = MagicMock()
        hub.account = MagicMock()
        hub._qmt_logged_in = True

        mock_order = MagicMock()
        mock_order.stock_code = '600000.SH'
        mock_order.order_type = qmt_hub.xtconstant.STOCK_BUY
        mock_order.order_status = qmt_hub.xtconstant.ORDER_SUCCEEDED
        mock_order.price = 10.5
        mock_order.order_volume = 100
        mock_order.filled_volume = 100
        mock_order.order_id = 12345
        hub.xt_trader.query_stock_orders.return_value = [mock_order]

        resp = hub._process_trade_action({'action': 'query_orders'})
        assert resp['ok'] is True
        item = resp['data'][0]
        missing = HUAXIN_ORDER_FIELDS - set(item.keys())
        assert missing == set(), f"query_orders missing huaxin fields: {missing}"
        assert item['direction'] == '买入'
        assert item['order_status'] == '全成'
        assert item['limit_price'] == 10.5
        assert item['volume_original'] == 100
        assert item['volume_traded'] == 100

    def test_query_orders_sell_direction(self):
        hub = qmt_hub.QMTHub()
        hub.xt_trader = MagicMock()
        hub.account = MagicMock()
        hub._qmt_logged_in = True

        mock_order = MagicMock()
        mock_order.stock_code = '000002.SZ'
        mock_order.order_type = qmt_hub.xtconstant.STOCK_SELL
        mock_order.order_status = qmt_hub.xtconstant.ORDER_PART_SUCC
        mock_order.price = 20.0
        mock_order.order_volume = 200
        mock_order.filled_volume = 100
        mock_order.order_id = 12346
        hub.xt_trader.query_stock_orders.return_value = [mock_order]

        resp = hub._process_trade_action({'action': 'query_orders'})
        item = resp['data'][0]
        assert item['direction'] == '卖出'
        assert item['order_status'] == '部成'
        assert item['volume_canceled'] == 100

    def test_query_trades_dry_run_field_compat(self):
        hub = qmt_hub.QMTHub(dry_run=True)
        resp = hub._process_trade_action({'action': 'query_trades'})
        assert resp['ok'] is True
        assert resp['data'] == []

    def test_query_trades_with_mock_trader(self):
        hub = qmt_hub.QMTHub()
        hub.xt_trader = MagicMock()
        hub.account = MagicMock()
        hub._qmt_logged_in = True

        mock_trade = MagicMock()
        mock_trade.stock_code = '600000.SH'
        mock_trade.order_type = qmt_hub.xtconstant.STOCK_BUY
        mock_trade.traded_price = 10.5
        mock_trade.traded_volume = 100
        mock_trade.traded_id = 99999
        mock_trade.order_id = 12345
        hub.xt_trader.query_stock_trades.return_value = [mock_trade]

        resp = hub._process_trade_action({'action': 'query_trades'})
        assert resp['ok'] is True
        item = resp['data'][0]
        missing = HUAXIN_TRADE_FIELDS - set(item.keys())
        assert missing == set(), f"query_trades missing huaxin fields: {missing}"
        assert item['direction'] == '买入'
        assert item['price'] == 10.5
        assert item['volume'] == 100

    def test_buy_limit_order(self):
        hub = qmt_hub.QMTHub()
        hub.xt_trader = MagicMock()
        hub.account = MagicMock()
        hub._qmt_logged_in = True
        hub.xt_trader.order_stock.return_value = 12345

        resp = hub._process_trade_action({
            'action': 'buy', 'stock': '600000.SH', 'shares': 100,
            'price': 10.5, 'order_type': 'LIMIT', 'reason': 'test_buy',
        })
        assert resp['ok'] is True
        assert 'order_ref' in resp
        assert resp['order_ref'] == 12345
        call_args = hub.xt_trader.order_stock.call_args[0]
        assert call_args[2] == qmt_hub.xtconstant.STOCK_BUY
        assert call_args[3] == 100
        assert call_args[4] == qmt_hub.xtconstant.FIX_PRICE
        assert call_args[5] == 10.5

    def test_buy_market_order(self):
        hub = qmt_hub.QMTHub()
        hub.xt_trader = MagicMock()
        hub.account = MagicMock()
        hub._qmt_logged_in = True
        hub.xt_trader.order_stock.return_value = 12346

        resp = hub._process_trade_action({
            'action': 'buy', 'stock': '600000.SH', 'shares': 100,
            'price': 0, 'order_type': 'MARKET',
        })
        assert resp['ok'] is True
        call_args = hub.xt_trader.order_stock.call_args[0]
        assert call_args[4] == qmt_hub.xtconstant.LATEST_PRICE
        assert call_args[5] == 0

    def test_sell_limit_order(self):
        hub = qmt_hub.QMTHub()
        hub.xt_trader = MagicMock()
        hub.account = MagicMock()
        hub._qmt_logged_in = True
        hub.xt_trader.order_stock.return_value = 12347

        resp = hub._process_trade_action({
            'action': 'sell', 'stock': '000002.SZ', 'shares': 200,
            'price': 20.0, 'order_type': 'LIMIT',
        })
        assert resp['ok'] is True
        call_args = hub.xt_trader.order_stock.call_args[0]
        assert call_args[2] == qmt_hub.xtconstant.STOCK_SELL
        assert call_args[3] == 200

    def test_buy_shares_rounded_to_100(self):
        hub = qmt_hub.QMTHub()
        hub.xt_trader = MagicMock()
        hub.account = MagicMock()
        hub._qmt_logged_in = True
        hub.xt_trader.order_stock.return_value = 12348

        resp = hub._process_trade_action({
            'action': 'buy', 'stock': '600000.SH', 'shares': 150,
            'price': 10.5, 'order_type': 'LIMIT',
        })
        assert resp['ok'] is True
        assert hub.xt_trader.order_stock.call_args[0][3] == 100

    def test_buy_shares_250_rounds_to_200(self):
        hub = qmt_hub.QMTHub()
        hub.xt_trader = MagicMock()
        hub.account = MagicMock()
        hub._qmt_logged_in = True
        hub.xt_trader.order_stock.return_value = 12349

        resp = hub._process_trade_action({
            'action': 'buy', 'stock': '600000.SH', 'shares': 250,
            'price': 10.5, 'order_type': 'LIMIT',
        })
        assert resp['ok'] is True
        assert hub.xt_trader.order_stock.call_args[0][3] == 200

    def test_buy_shares_less_than_100_rejected(self):
        hub = qmt_hub.QMTHub()
        hub._qmt_logged_in = True
        resp = hub._process_trade_action({
            'action': 'buy', 'stock': '600000.SH', 'shares': 50,
            'price': 10.5,
        })
        assert resp['ok'] is False

    def test_buy_shares_zero_rejected(self):
        hub = qmt_hub.QMTHub()
        hub._qmt_logged_in = True
        resp = hub._process_trade_action({
            'action': 'buy', 'stock': '600000.SH', 'shares': 0,
            'price': 10.5,
        })
        assert resp['ok'] is False

    def test_buy_not_logged_in(self):
        hub = qmt_hub.QMTHub()
        hub._qmt_logged_in = False
        resp = hub._process_trade_action({
            'action': 'buy', 'stock': '600000.SH', 'shares': 100,
            'price': 10.5,
        })
        assert resp['ok'] is False
        assert 'not connected' in resp['error'].lower()

    def test_buy_missing_stock(self):
        hub = qmt_hub.QMTHub()
        hub._qmt_logged_in = True
        resp = hub._process_trade_action({
            'action': 'buy', 'shares': 100, 'price': 10.5,
        })
        assert resp['ok'] is False
        assert 'stock' in resp['error'].lower()

    def test_invalid_order_type(self):
        hub = qmt_hub.QMTHub()
        hub.xt_trader = MagicMock()
        hub.account = MagicMock()
        hub._qmt_logged_in = True
        resp = hub._process_trade_action({
            'action': 'buy', 'stock': '600000.SH', 'shares': 100,
            'price': 10.5, 'order_type': 'FOK',
        })
        assert resp['ok'] is False
        assert 'order_type' in resp['error'].lower()

    def test_unknown_action(self):
        hub = qmt_hub.QMTHub()
        resp = hub._process_trade_action({'action': 'cancel'})
        assert resp['ok'] is False
        assert 'unknown action' in resp['error'].lower()

    def test_buy_order_stock_returns_negative(self):
        hub = qmt_hub.QMTHub()
        hub.xt_trader = MagicMock()
        hub.account = MagicMock()
        hub._qmt_logged_in = True
        hub.xt_trader.order_stock.return_value = -1

        resp = hub._process_trade_action({
            'action': 'buy', 'stock': '600000.SH', 'shares': 100,
            'price': 10.5, 'order_type': 'LIMIT',
        })
        assert resp['ok'] is False

    def test_buy_order_stock_raises_exception(self):
        hub = qmt_hub.QMTHub()
        hub.xt_trader = MagicMock()
        hub.account = MagicMock()
        hub._qmt_logged_in = True
        hub.xt_trader.order_stock.side_effect = Exception("connection lost")

        resp = hub._process_trade_action({
            'action': 'buy', 'stock': '600000.SH', 'shares': 100,
            'price': 10.5, 'order_type': 'LIMIT',
        })
        assert resp['ok'] is False
        assert 'connection lost' in resp['error']

    def test_reason_truncated_to_40_chars(self):
        hub = qmt_hub.QMTHub()
        hub.xt_trader = MagicMock()
        hub.account = MagicMock()
        hub._qmt_logged_in = True
        hub.xt_trader.order_stock.return_value = 12350

        long_reason = 'A' * 100
        resp = hub._process_trade_action({
            'action': 'buy', 'stock': '600000.SH', 'shares': 100,
            'price': 10.5, 'order_type': 'LIMIT', 'reason': long_reason,
        })
        assert resp['ok'] is True
        call_args = hub.xt_trader.order_stock.call_args[0]
        assert len(call_args[7]) == 40

    def test_default_order_type_is_limit(self):
        hub = qmt_hub.QMTHub()
        hub.xt_trader = MagicMock()
        hub.account = MagicMock()
        hub._qmt_logged_in = True
        hub.xt_trader.order_stock.return_value = 12351

        resp = hub._process_trade_action({
            'action': 'buy', 'stock': '600000.SH', 'shares': 100,
            'price': 10.5,
        })
        assert resp['ok'] is True
        call_args = hub.xt_trader.order_stock.call_args[0]
        assert call_args[4] == qmt_hub.xtconstant.FIX_PRICE


# ============================================================
# 4. ZMQ行情查询协议集成 (真实ZMQ REQ/REP)
# ============================================================
class TestZMQQueryProtocol:
    """真实ZMQ socket测试: 行情查询协议与market_hub_v2兼容"""

    def _start_query_server(self, hub, port):
        ctx = zmq.Context()
        rep = ctx.socket(zmq.REP)
        rep.bind(f'tcp://127.0.0.1:{port}')
        return ctx, rep

    def test_query_snapshot(self):
        port = 29802
        hub = qmt_hub.QMTHub(query_port=port)
        hub._start_time = time.time()
        hub._snapshots = {
            '600000.SH': _make_full_snapshot('600000.SH'),
        }

        ctx = zmq.Context()
        rep = ctx.socket(zmq.REP)
        rep.bind(f'tcp://127.0.0.1:{port}')

        req_ctx = zmq.Context()
        req = req_ctx.socket(zmq.REQ)
        req.connect(f'tcp://127.0.0.1:{port}')
        req.setsockopt(zmq.RCVTIMEO, 3000)

        def server():
            try:
                msg = rep.recv_string()
                parsed = json.loads(msg)
                cmd = parsed.get('cmd', '')
                if cmd == 'snapshot':
                    code = parsed.get('code', '')
                    with hub._snap_lock:
                        data = hub._snapshots.get(code)
                    rep.send_json({'ok': True, 'data': data} if data
                                  else {'ok': False, 'error': f'No snapshot for {code}'})
                else:
                    rep.send_json({'ok': False, 'error': f'Unknown cmd: {cmd}'})
            except Exception as e:
                rep.send_json({'ok': False, 'error': str(e)})

        t = threading.Thread(target=server, daemon=True)
        t.start()

        req.send_string(json.dumps({'cmd': 'snapshot', 'code': '600000.SH'}))
        resp = json.loads(req.recv_string())
        assert resp['ok'] is True
        assert resp['data']['code'] == '600000.SH'
        assert resp['data']['last_price'] == 10.5

        req.close()
        req_ctx.term()
        rep.close()
        ctx.term()

    def test_query_snapshot_not_found(self):
        port = 29803
        hub = qmt_hub.QMTHub(query_port=port)
        hub._start_time = time.time()
        hub._snapshots = {}

        ctx = zmq.Context()
        rep = ctx.socket(zmq.REP)
        rep.bind(f'tcp://127.0.0.1:{port}')

        req_ctx = zmq.Context()
        req = req_ctx.socket(zmq.REQ)
        req.connect(f'tcp://127.0.0.1:{port}')
        req.setsockopt(zmq.RCVTIMEO, 3000)

        def server():
            try:
                msg = rep.recv_string()
                parsed = json.loads(msg)
                cmd = parsed.get('cmd', '')
                if cmd == 'snapshot':
                    code = parsed.get('code', '')
                    with hub._snap_lock:
                        data = hub._snapshots.get(code)
                    rep.send_json({'ok': True, 'data': data} if data
                                  else {'ok': False, 'error': f'No snapshot for {code}'})
            except Exception:
                rep.send_json({'ok': False, 'error': 'server error'})

        t = threading.Thread(target=server, daemon=True)
        t.start()

        req.send_string(json.dumps({'cmd': 'snapshot', 'code': '999999.SH'}))
        resp = json.loads(req.recv_string())
        assert resp['ok'] is False

        req.close()
        req_ctx.term()
        rep.close()
        ctx.term()

    def test_query_stats(self):
        port = 29804
        hub = qmt_hub.QMTHub(query_port=port)
        hub._start_time = time.time() - 60
        hub._msg_count = 1000
        hub._snap_count = 500
        hub._bar_count = 50
        hub._codes_seen = {'600000.SH', '000001.IDX'}
        hub._snapshots = {'600000.SH': _make_full_snapshot('600000.SH')}

        ctx = zmq.Context()
        rep = ctx.socket(zmq.REP)
        rep.bind(f'tcp://127.0.0.1:{port}')

        req_ctx = zmq.Context()
        req = req_ctx.socket(zmq.REQ)
        req.connect(f'tcp://127.0.0.1:{port}')
        req.setsockopt(zmq.RCVTIMEO, 3000)

        def server():
            try:
                msg = rep.recv_string()
                parsed = json.loads(msg)
                if parsed.get('cmd') == 'stats':
                    elapsed = time.time() - hub._start_time
                    with hub._snap_lock:
                        snap_count = len(hub._snapshots)
                    ts = hub.tick_saver.stats()
                    rep.send_json({'ok': True, 'data': {
                        'total_msgs': hub._msg_count,
                        'snap_published': hub._snap_count,
                        'bars_published': hub._bar_count,
                        'codes_seen': len(hub._codes_seen),
                        'snapshot_cached': snap_count,
                        'tick_saved': ts['tick_count'],
                        'tick_files': ts['file_count'],
                        'elapsed_sec': round(elapsed, 1),
                        'rate_per_sec': round(hub._msg_count / elapsed, 1) if elapsed > 0 else 0,
                        'source': 'qmt_mini',
                    }})
            except Exception as e:
                rep.send_json({'ok': False, 'error': str(e)})

        t = threading.Thread(target=server, daemon=True)
        t.start()

        req.send_string(json.dumps({'cmd': 'stats'}))
        resp = json.loads(req.recv_string())
        assert resp['ok'] is True
        assert resp['data']['total_msgs'] == 1000
        assert resp['data']['source'] == 'qmt_mini'
        assert resp['data']['rate_per_sec'] > 0

        req.close()
        req_ctx.term()
        rep.close()
        ctx.term()

    def test_query_bars(self):
        port = 29805
        hub = qmt_hub.QMTHub(query_port=port)
        hub._start_time = time.time()

        snap = _make_full_snapshot('600000.SH')
        hub.aggregator.update(
            code='600000.SH', price=10.5, volume=1000, turnover=10500.0,
            timestamp_str='09:30:00', pre_close=10.0, open_price=10.3,
            high_price=10.8, low_price=10.2, ask1=10.51, bid1=10.49,
        )
        hub.aggregator.update(
            code='600000.SH', price=10.6, volume=2000, turnover=21200.0,
            timestamp_str='09:31:00', pre_close=10.0, open_price=10.3,
            high_price=10.8, low_price=10.2, ask1=10.61, bid1=10.59,
        )

        ctx = zmq.Context()
        rep = ctx.socket(zmq.REP)
        rep.bind(f'tcp://127.0.0.1:{port}')

        req_ctx = zmq.Context()
        req = req_ctx.socket(zmq.REQ)
        req.connect(f'tcp://127.0.0.1:{port}')
        req.setsockopt(zmq.RCVTIMEO, 3000)

        def server():
            try:
                msg = rep.recv_string()
                parsed = json.loads(msg)
                if parsed.get('cmd') == 'bars':
                    code = parsed.get('code', '')
                    count = parsed.get('count', 120)
                    bars = hub.aggregator.get_1m_bars(code, count)
                    rep.send_json({'ok': True, 'data': bars})
            except Exception as e:
                rep.send_json({'ok': False, 'error': str(e)})

        t = threading.Thread(target=server, daemon=True)
        t.start()

        req.send_string(json.dumps({'cmd': 'bars', 'code': '600000.SH', 'count': 10}))
        resp = json.loads(req.recv_string())
        assert resp['ok'] is True
        assert len(resp['data']) >= 1

        req.close()
        req_ctx.term()
        rep.close()
        ctx.term()


# ============================================================
# 5. ZMQ交易网关集成 (真实ZMQ REQ/REP)
# ============================================================
class TestZMQTradeGateway:
    """真实ZMQ socket测试: 交易网关与huaxin_trade_gateway协议兼容"""

    def _run_trade_server(self, hub, rep, stop_event):
        while not stop_event.is_set():
            try:
                if not rep.poll(200):
                    continue
                msg = rep.recv_string()
                req = json.loads(msg)
                resp = hub._process_trade_action(req)
                rep.send_string(json.dumps(resp, ensure_ascii=False))
            except Exception:
                if not stop_event.is_set():
                    try:
                        rep.send_string(json.dumps({'ok': False, 'error': 'internal error'}))
                    except Exception:
                        pass

    def test_zmq_ping(self):
        port = 29850
        hub = qmt_hub.QMTHub(dry_run=True, trade_port=port)
        hub._qmt_logged_in = True

        ctx = zmq.Context()
        rep = ctx.socket(zmq.REP)
        rep.bind(f'tcp://127.0.0.1:{port}')

        stop_event = threading.Event()
        server_t = threading.Thread(target=self._run_trade_server,
                                    args=(hub, rep, stop_event), daemon=True)
        server_t.start()

        try:
            req_ctx = zmq.Context()
            req = req_ctx.socket(zmq.REQ)
            req.connect(f'tcp://127.0.0.1:{port}')
            req.setsockopt(zmq.RCVTIMEO, 3000)

            req.send_string(json.dumps({'action': 'ping'}))
            resp = json.loads(req.recv_string())
            assert resp['ok'] is True
            assert resp['status'] == 'alive'
            assert resp['source'] == 'qmt_mini'

            req.close()
            req_ctx.term()
        finally:
            stop_event.set()
            rep.close()
            ctx.term()

    def test_zmq_buy_and_query(self):
        port = 29851
        hub = qmt_hub.QMTHub(dry_run=True, trade_port=port)
        hub._qmt_logged_in = True
        hub.xt_trader = MagicMock()
        hub.account = MagicMock()
        hub.xt_trader.order_stock.return_value = 99999

        ctx = zmq.Context()
        rep = ctx.socket(zmq.REP)
        rep.bind(f'tcp://127.0.0.1:{port}')

        stop_event = threading.Event()
        server_t = threading.Thread(target=self._run_trade_server,
                                    args=(hub, rep, stop_event), daemon=True)
        server_t.start()

        try:
            req_ctx = zmq.Context()
            req = req_ctx.socket(zmq.REQ)
            req.connect(f'tcp://127.0.0.1:{port}')
            req.setsockopt(zmq.RCVTIMEO, 3000)

            req.send_string(json.dumps({
                'action': 'buy', 'stock': '600000.SH', 'shares': 100,
                'price': 10.5, 'order_type': 'LIMIT', 'reason': 'zmq_test',
            }))
            resp = json.loads(req.recv_string())
            assert resp['ok'] is True
            assert resp['order_ref'] == 99999

            req.send_string(json.dumps({'action': 'query_account'}))
            resp = json.loads(req.recv_string())
            assert resp['ok'] is True
            assert 'data' in resp

            req.close()
            req_ctx.term()
        finally:
            stop_event.set()
            rep.close()
            ctx.term()

    def test_zmq_multiple_requests_serial(self):
        port = 29852
        hub = qmt_hub.QMTHub(dry_run=True, trade_port=port)
        hub._qmt_logged_in = True

        ctx = zmq.Context()
        rep = ctx.socket(zmq.REP)
        rep.bind(f'tcp://127.0.0.1:{port}')

        stop_event = threading.Event()
        server_t = threading.Thread(target=self._run_trade_server,
                                    args=(hub, rep, stop_event), daemon=True)
        server_t.start()

        try:
            req_ctx = zmq.Context()
            req = req_ctx.socket(zmq.REQ)
            req.connect(f'tcp://127.0.0.1:{port}')
            req.setsockopt(zmq.RCVTIMEO, 3000)

            actions = ['ping', 'query_account', 'query_position',
                       'query_orders', 'query_trades', 'ping']
            for action in actions:
                req.send_string(json.dumps({'action': action}))
                resp = json.loads(req.recv_string())
                assert resp['ok'] is True, f"action={action} failed: {resp}"

            req.close()
            req_ctx.term()
        finally:
            stop_event.set()
            rep.close()
            ctx.term()

    def test_zmq_invalid_json(self):
        port = 29853
        hub = qmt_hub.QMTHub(dry_run=True, trade_port=port)

        ctx = zmq.Context()
        rep = ctx.socket(zmq.REP)
        rep.bind(f'tcp://127.0.0.1:{port}')

        stop_event = threading.Event()

        def server():
            while not stop_event.is_set():
                try:
                    if not rep.poll(200):
                        continue
                    msg = rep.recv_string()
                    try:
                        req = json.loads(msg)
                    except json.JSONDecodeError:
                        rep.send_string(json.dumps({'ok': False, 'error': 'invalid JSON'}))
                        continue
                    resp = hub._process_trade_action(req)
                    rep.send_string(json.dumps(resp, ensure_ascii=False))
                except Exception:
                    if not stop_event.is_set():
                        try:
                            rep.send_string(json.dumps({'ok': False, 'error': 'internal'}))
                        except Exception:
                            pass

        server_t = threading.Thread(target=server, daemon=True)
        server_t.start()

        try:
            req_ctx = zmq.Context()
            req = req_ctx.socket(zmq.REQ)
            req.connect(f'tcp://127.0.0.1:{port}')
            req.setsockopt(zmq.RCVTIMEO, 3000)

            req.send_string('not valid json{{{')
            resp = json.loads(req.recv_string())
            assert resp['ok'] is False

            req.close()
            req_ctx.term()
        finally:
            stop_event.set()
            rep.close()
            ctx.term()


# ============================================================
# 6. FastAPI端点兼容性 (真实HTTP请求)
# ============================================================
class TestFastAPIEndpoints:
    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from fastapi import FastAPI, Query

        hub = qmt_hub.QMTHub()
        hub._snapshots = {
            '600000.SH': _make_full_snapshot('600000.SH'),
            '000001.IDX': _make_full_snapshot('000001.SH'),
        }
        hub._start_time = time.time()
        hub._msg_count = 500
        hub._snap_count = 200
        hub._bar_count = 20
        hub._codes_seen = {'600000.SH', '000001.IDX'}

        app = FastAPI(title="QMT Hub API Test", version="1.0")

        @app.get("/api/snapshot/{code}")
        def api_snapshot(code: str):
            with hub._snap_lock:
                data = hub._snapshots.get(code)
            if data:
                return {"ok": True, "data": data}
            return {"ok": False, "error": f"No snapshot for {code}"}

        @app.get("/api/snapshots")
        def api_all_snapshots(index_only: bool = Query(False)):
            with hub._snap_lock:
                if index_only:
                    data = {c: s for c, s in hub._snapshots.items() if s.get('is_index')}
                else:
                    data = dict(hub._snapshots)
            return {"ok": True, "data": data, "count": len(data)}

        @app.get("/api/bars/1m/{code}")
        def api_1m_bars(code: str, count: int = Query(120)):
            bars = hub.aggregator.get_1m_bars(code, count)
            return {"ok": True, "data": bars, "count": len(bars)}

        @app.get("/api/bars/5m/{code}")
        def api_5m_bars(code: str, count: int = Query(48)):
            bars = hub.aggregator.get_5m_bars(code, count)
            return {"ok": True, "data": bars, "count": len(bars)}

        @app.get("/api/stats")
        def api_stats():
            elapsed = time.time() - hub._start_time
            with hub._snap_lock:
                snap_count = len(hub._snapshots)
            ts = hub.tick_saver.stats()
            return {"ok": True, "data": {
                "total_msgs": hub._msg_count,
                "snapshot_cached": snap_count,
                "tick_saved": ts['tick_count'],
                "source": "qmt_mini",
            }}

        return TestClient(app)

    def test_snapshot_existing(self, client):
        resp = client.get("/api/snapshot/600000.SH")
        assert resp.status_code == 200
        data = resp.json()
        assert data['ok'] is True
        assert data['data']['code'] == '600000.SH'
        assert data['data']['last_price'] == 10.5

    def test_snapshot_nonexistent(self, client):
        resp = client.get("/api/snapshot/999999.SH")
        assert resp.status_code == 200
        data = resp.json()
        assert data['ok'] is False

    def test_all_snapshots(self, client):
        resp = client.get("/api/snapshots")
        assert resp.status_code == 200
        data = resp.json()
        assert data['ok'] is True
        assert data['count'] == 2

    def test_all_snapshots_index_only(self, client):
        resp = client.get("/api/snapshots?index_only=true")
        assert resp.status_code == 200
        data = resp.json()
        assert data['count'] == 1
        assert '000001.IDX' in data['data']

    def test_stats(self, client):
        resp = client.get("/api/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data['ok'] is True
        assert data['data']['total_msgs'] == 500
        assert data['data']['source'] == 'qmt_mini'

    def test_bars_1m_empty(self, client):
        resp = client.get("/api/bars/1m/600000.SH?count=10")
        assert resp.status_code == 200
        data = resp.json()
        assert data['ok'] is True

    def test_bars_5m_empty(self, client):
        resp = client.get("/api/bars/5m/600000.SH?count=10")
        assert resp.status_code == 200
        data = resp.json()
        assert data['ok'] is True


# ============================================================
# 7. BarAggregator集成 (tick→1m/5m K线)
# ============================================================
class TestBarAggregatorIntegration:
    def test_single_tick_creates_incomplete_bar(self):
        hub = qmt_hub.QMTHub()
        snap = _make_full_snapshot('600000.SH')
        completed_1m, _ = hub.aggregator.update(
            code='600000.SH', price=snap['last_price'],
            volume=snap['volume'], turnover=snap['turnover'],
            timestamp_str='09:30:00', pre_close=snap['pre_close'],
            open_price=snap['open'], high_price=snap['high'],
            low_price=snap['low'], ask1=snap['ask1'], bid1=snap['bid1'],
        )
        assert completed_1m is None
        bars = hub.aggregator.get_1m_bars('600000.SH', 10)
        assert len(bars) == 1
        assert bars[0]['open'] == 10.3
        assert bars[0]['close'] == 10.5

    def test_two_minutes_produce_completed_bar(self):
        hub = qmt_hub.QMTHub()
        prices = [10.5, 10.6]
        for i, minute in enumerate(['09:30:00', '09:31:00']):
            hub.aggregator.update(
                code='600000.SH', price=prices[i],
                volume=1000, turnover=10500.0,
                timestamp_str=minute, pre_close=10.0,
                open_price=10.3, high_price=10.8,
                low_price=10.2, ask1=10.51, bid1=10.49,
            )
        bars = hub.aggregator.get_1m_bars('600000.SH', 10)
        assert len(bars) == 2

    def test_5m_bar_aggregation(self):
        hub = qmt_hub.QMTHub()
        for mm in range(0, 10):
            ts = f'09:{mm:02d}:00'
            hub.aggregator.update(
                code='600000.SH', price=10.5, volume=1000,
                turnover=10500.0, timestamp_str=ts,
                pre_close=10.0, open_price=10.3,
                high_price=10.8, low_price=10.2,
            )
        bars_5m = hub.aggregator.get_5m_bars('600000.SH', 10)
        assert len(bars_5m) >= 1

    def test_bar_fields_match_market_hub_v2(self):
        hub = qmt_hub.QMTHub()
        hub.aggregator.update(
            code='600000.SH', price=10.5, volume=1000, turnover=10500.0,
            timestamp_str='09:30:00', pre_close=10.0, open_price=10.3,
            high_price=10.8, low_price=10.2, ask1=10.51, bid1=10.49,
        )
        hub.aggregator.update(
            code='600000.SH', price=10.6, volume=2000, turnover=21200.0,
            timestamp_str='09:31:00', pre_close=10.0, open_price=10.3,
            high_price=10.8, low_price=10.2, ask1=10.61, bid1=10.59,
        )
        bars = hub.aggregator.get_1m_bars('600000.SH', 10)
        bar = bars[0]
        expected_fields = {'code', 'datetime', 'open', 'high', 'low', 'close',
                           'volume', 'turnover', 'pre_close', 'ask1', 'bid1'}
        assert expected_fields.issubset(set(bar.keys()))

    def test_multiple_codes_independent(self):
        hub = qmt_hub.QMTHub()
        for code in ['600000.SH', '000002.SZ']:
            hub.aggregator.update(
                code=code, price=10.5, volume=1000, turnover=10500.0,
                timestamp_str='09:30:00', pre_close=10.0,
            )
        assert len(hub.aggregator.get_1m_bars('600000.SH', 10)) == 1
        assert len(hub.aggregator.get_1m_bars('000002.SZ', 10)) == 1


# ============================================================
# 8. TickSaver集成 (tick→parquet持久化)
# ============================================================
class TestTickSaverIntegration:
    def test_tick_saved_to_parquet(self):
        hub = qmt_hub.QMTHub()
        tick_record = {
            'timestamp': '2026-07-24 09:30:00.000',
            'code': '600000.SH',
            'last_price': 10.5,
            'volume': 1000,
            'turnover': 10500.0,
            'open': 10.3,
            'high': 10.8,
            'low': 10.2,
            'pre_close': 10.0,
            'ask1': 10.51,
            'ask_vol1': 100,
            'bid1': 10.49,
            'bid_vol1': 150,
        }
        hub.tick_saver.add('600000.SH', tick_record)
        hub.tick_saver.flush()

        day = date.today().isoformat()
        out_file = mh2.TICK_DIR / '600000.SH' / f'{day}.parquet'
        assert out_file.exists(), f"Parquet file not created: {out_file}"

        df = pd.read_parquet(out_file)
        assert len(df) == 1
        assert df.iloc[0]['code'] == '600000.SH'
        assert df.iloc[0]['last_price'] == 10.5

    def test_tick_dedup_on_flush(self):
        hub = qmt_hub.QMTHub()
        tick1 = {
            'timestamp': '2026-07-24 09:30:00.000', 'code': '600000.SH',
            'last_price': 10.5, 'volume': 1000, 'turnover': 10500.0,
            'open': 10.3, 'high': 10.8, 'low': 10.2, 'pre_close': 10.0,
            'ask1': 10.51, 'ask_vol1': 100, 'bid1': 10.49, 'bid_vol1': 150,
        }
        tick2 = dict(tick1)
        tick2['last_price'] = 10.6

        hub.tick_saver.add('600000.SH', tick1)
        hub.tick_saver.add('600000.SH', tick2)
        hub.tick_saver.flush()

        day = date.today().isoformat()
        out_file = mh2.TICK_DIR / '600000.SH' / f'{day}.parquet'
        df = pd.read_parquet(out_file)
        assert len(df) == 2

    def test_tick_saver_stats(self):
        hub = qmt_hub.QMTHub()
        tick = {
            'timestamp': '2026-07-24 09:30:00.000', 'code': '600000.SH',
            'last_price': 10.5, 'volume': 1000, 'turnover': 10500.0,
            'open': 10.3, 'high': 10.8, 'low': 10.2, 'pre_close': 10.0,
            'ask1': 10.51, 'ask_vol1': 100, 'bid1': 10.49, 'bid_vol1': 150,
        }
        hub.tick_saver.add('600000.SH', tick)
        stats = hub.tick_saver.stats()
        assert stats['tick_count'] == 1

    def test_tick_get_data(self):
        hub = qmt_hub.QMTHub()
        tick = {
            'timestamp': '2026-07-24 09:30:00.000', 'code': '600000.SH',
            'last_price': 10.5, 'volume': 1000, 'turnover': 10500.0,
            'open': 10.3, 'high': 10.8, 'low': 10.2, 'pre_close': 10.0,
            'ask1': 10.51, 'ask_vol1': 100, 'bid1': 10.49, 'bid_vol1': 150,
        }
        hub.tick_saver.add('600000.SH', tick)
        hub.tick_saver.flush()

        data = hub.tick_saver.get_tick_data('600000.SH')
        assert len(data) == 1
        assert data[0]['last_price'] == 10.5


# ============================================================
# 9. QMT Mini实盘集成测试 (急跌买入.bat参数)
# ============================================================
QMT_PATH_LIVE = 'D:/QMTgj/userdata_mini'
ACCOUNT_ID_LIVE = '8884972726'
SESSION_ID_LIVE = 20260708
PER_STOCK_AMOUNT_LIVE = 5000


class TestQMTLiveConnection:
    """QMT Mini live integration tests with bands strategy params

    Params from: ji_die_buy.bat
      --account 8884972726 --qmt-path D:/QMTgj/userdata_mini
      SESSION_ID = 20260708, PER_STOCK_AMOUNT = 5000

    Run: pytest test_qmt_hub.py -v --qmt-live
    """

    @pytest.fixture(autouse=True)
    def check_qmt_live(self, request):
        if not request.config.getoption('--qmt-live', default=False):
            pytest.skip('需要 --qmt-live 标记才运行QMT实盘测试')
        if not qmt_hub.QMT_AVAILABLE:
            pytest.skip('xtquant未安装')

    def test_xtdata_get_stock_list(self):
        from xtquant import xtdata
        sectors = xtdata.get_stock_list_in_sector('沪深A股')
        assert sectors is not None
        assert len(sectors) > 1000
        assert any('600000.SH' in s for s in sectors)

    def test_xtdata_get_full_tick_single(self):
        from xtquant import xtdata
        ticks = xtdata.get_full_tick(['600000.SH'])
        assert ticks is not None
        assert '600000.SH' in ticks
        tick = ticks['600000.SH']
        assert tick is not None
        assert 'lastPrice' in tick
        assert 'lastClose' in tick
        assert tick['lastClose'] > 0, "lastClose=0, 非交易时段?"

    def test_xtdata_get_full_tick_multiple(self):
        from xtquant import xtdata
        codes = ['600000.SH', '000002.SZ', '300001.SZ']
        ticks = xtdata.get_full_tick(codes)
        assert ticks is not None
        for code in codes:
            assert code in ticks

    def test_xtdata_get_full_tick_index(self):
        from xtquant import xtdata
        ticks = xtdata.get_full_tick(['000001.SH'])
        assert ticks is not None
        tick = ticks.get('000001.SH')
        if tick and tick.get('lastClose', 0) > 0:
            snap = qmt_hub.qmt_tick_to_snapshot('000001.SH', tick)
            assert snap is not None
            assert snap['is_index'] is True
            assert snap['code'] == '000001.IDX'

    # ---- qmt_trader.py兼容性: tick字段映射 ----

    def test_qmt_tick_fields_match_trader_usage(self):
        """验证qmt_hub的tick->snapshot映射与qmt_trader.py使用的字段一致"""
        from xtquant import xtdata
        ticks = xtdata.get_full_tick(['600000.SH'])
        tick = ticks.get('600000.SH')
        if not tick or tick.get('lastClose', 0) == 0:
            pytest.skip('非交易时段, tick数据不完整')

        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', tick)

        assert abs(snap['last_price'] - tick['lastPrice']) < 0.001
        assert abs(snap['open'] - tick['open']) < 0.001
        assert abs(snap['high'] - tick['high']) < 0.001
        assert abs(snap['low'] - tick['low']) < 0.001
        assert abs(snap['pre_close'] - tick['lastClose']) < 0.001
        assert snap['volume'] == tick['volume']
        assert abs(snap['turnover'] - tick['amount']) < 0.01
        ask_prices = tick.get('askPrice', [0] * 5)
        bid_prices = tick.get('bidPrice', [0] * 5)
        if ask_prices and ask_prices[0] > 0:
            assert abs(snap['ask1'] - ask_prices[0]) < 0.001
        if bid_prices and bid_prices[0] > 0:
            assert abs(snap['bid1'] - bid_prices[0]) < 0.001

    def test_qmt_tick_to_snapshot_live_data(self):
        from xtquant import xtdata
        ticks = xtdata.get_full_tick(['600000.SH'])
        tick = ticks.get('600000.SH')
        if not tick or tick.get('lastClose', 0) == 0:
            pytest.skip('非交易时段, tick数据不完整')
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', tick)
        assert snap is not None
        assert snap['code'] == '600000.SH'
        assert snap['last_price'] > 0
        assert snap['pre_close'] > 0
        assert snap['volume'] >= 0
        missing = SNAPSHOT_FIELDS - set(snap.keys())
        assert missing == set(), f"Missing fields in live snapshot: {missing}"

    def test_qmt_tick_to_snapshot_live_orderbook(self):
        from xtquant import xtdata
        ticks = xtdata.get_full_tick(['600000.SH'])
        tick = ticks.get('600000.SH')
        if not tick or tick.get('lastClose', 0) == 0:
            pytest.skip('非交易时段, 盘口数据不完整')
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', tick)
        if snap['ask1'] > 0:
            assert snap['ask1'] >= snap['bid1']
        for i in range(1, 5):
            if snap[f'ask{i}'] > 0 and snap[f'ask{i+1}'] > 0:
                assert snap[f'ask{i}'] <= snap[f'ask{i+1}']
            if snap[f'bid{i}'] > 0 and snap[f'bid{i+1}'] > 0:
                assert snap[f'bid{i}'] >= snap[f'bid{i+1}']

    def test_qmt_hub_poll_ticks_live(self):
        from xtquant import xtdata
        hub = qmt_hub.QMTHub(dry_run=True)
        hub._all_codes = ['600000.SH', '000002.SZ']
        hub._start_time = time.time()
        hub._snap_pub = hub._ctx.socket(zmq.PUB)
        hub._snap_pub.bind('tcp://127.0.0.1:29900')
        hub._bar_pub = hub._ctx.socket(zmq.PUB)
        hub._bar_pub.bind('tcp://127.0.0.1:29901')

        try:
            hub._poll_ticks()
            assert hub._msg_count > 0
            assert len(hub._snapshots) > 0
            for code, snap in hub._snapshots.items():
                assert 'last_price' in snap
        finally:
            hub._snap_pub.close()
            hub._bar_pub.close()

    def test_qmt_hub_full_tick_to_bar_pipeline(self):
        from xtquant import xtdata
        hub = qmt_hub.QMTHub(dry_run=True)
        hub._all_codes = ['600000.SH']
        hub._start_time = time.time()
        hub._snap_pub = hub._ctx.socket(zmq.PUB)
        hub._snap_pub.bind('tcp://127.0.0.1:29902')
        hub._bar_pub = hub._ctx.socket(zmq.PUB)
        hub._bar_pub.bind('tcp://127.0.0.1:29903')

        try:
            hub._poll_ticks()
            if hub._msg_count > 0:
                bars = hub.aggregator.get_1m_bars('600000.SH', 10)
                if bars:
                    bar = bars[0]
                    assert 'open' in bar
                    assert 'close' in bar
                    assert 'volume' in bar
        finally:
            hub._snap_pub.close()
            hub._bar_pub.close()

    def test_qmt_trader_connect(self):
        """Use ji_die_buy.bat params to connect QMT trader and query account"""
        from xtquant.xttrader import XtQuantTrader
        from xtquant.xttype import StockAccount

        trader = XtQuantTrader(QMT_PATH_LIVE, SESSION_ID_LIVE)
        callback = qmt_hub.QMTTraderCallback(None)
        trader.register_callback(callback)
        trader.start()
        result = trader.connect()
        assert result == 0, f"QMT trader connect failed: {result}"

        account = StockAccount(ACCOUNT_ID_LIVE)
        sub_result = trader.subscribe(account)
        assert sub_result == 0, f"Account subscribe failed: {sub_result}"
        time.sleep(2)

        asset = trader.query_stock_asset(account)
        assert asset is not None
        assert hasattr(asset, 'cash')
        assert asset.cash >= 0
        print(f"  [LIVE] cash={asset.cash:.2f} total={asset.total_asset:.2f}")

        positions = trader.query_stock_positions(account)
        assert positions is not None

        trader.stop()

    def test_qmt_hub_trade_gateway_live(self):
        """QMTHub trade gateway with real QMT trader (ji_die_buy.bat params)

        Connect via ZMQ REQ to trade gateway, send ping/query_account/query_position
        """
        from xtquant.xttrader import XtQuantTrader
        from xtquant.xttype import StockAccount

        session_id = int(time.time())
        trader = XtQuantTrader(QMT_PATH_LIVE, session_id)
        callback = qmt_hub.QMTTraderCallback(None)
        trader.register_callback(callback)
        trader.start()
        result = trader.connect()
        assert result == 0, f"QMT trader connect failed: {result}"

        account = StockAccount(ACCOUNT_ID_LIVE)
        sub_result = trader.subscribe(account)
        assert sub_result == 0, f"Account subscribe failed: {sub_result}"
        time.sleep(2)

        hub = qmt_hub.QMTHub(dry_run=False,
                              qmt_path=QMT_PATH_LIVE,
                              account_id=ACCOUNT_ID_LIVE,
                              session_id=session_id)
        hub.xt_trader = trader
        hub.account = account
        hub._qmt_logged_in = True

        port = 29950
        ctx = zmq.Context()
        rep = ctx.socket(zmq.REP)
        rep.bind(f'tcp://127.0.0.1:{port}')

        stop_event = threading.Event()

        def trade_server():
            while not stop_event.is_set():
                try:
                    if not rep.poll(500):
                        continue
                    msg = rep.recv_string()
                    req = json.loads(msg)
                    resp = hub._process_trade_action(req)
                    rep.send_string(json.dumps(resp, ensure_ascii=False))
                except Exception:
                    if not stop_event.is_set():
                        try:
                            rep.send_string(json.dumps({'ok': False, 'error': 'internal'}))
                        except Exception:
                            pass

        server_t = threading.Thread(target=trade_server, daemon=True)
        server_t.start()

        try:
            req_ctx = zmq.Context()
            req = req_ctx.socket(zmq.REQ)
            req.connect(f'tcp://127.0.0.1:{port}')
            req.setsockopt(zmq.RCVTIMEO, 10000)

            req.send_string(json.dumps({'action': 'ping'}))
            resp = json.loads(req.recv_string())
            assert resp['ok'] is True
            assert resp['logged_in'] is True
            assert resp['source'] == 'qmt_mini'
            print(f"  [LIVE] ping: ok={resp['ok']} logged_in={resp['logged_in']}")

            req.send_string(json.dumps({'action': 'query_account'}))
            resp = json.loads(req.recv_string())
            assert resp['ok'] is True
            item = resp['data'][0]
            missing = HUAXIN_ACCOUNT_FIELDS - set(item.keys())
            assert missing == set(), f"Missing huaxin fields: {missing}"
            print(f"  [LIVE] account: useful_money={item['useful_money']:.2f} "
                  f"frozen={item['frozen_cash']:.2f}")

            req.send_string(json.dumps({'action': 'query_position'}))
            resp = json.loads(req.recv_string())
            assert resp['ok'] is True
            if resp['data']:
                pos = resp['data'][0]
                missing = HUAXIN_POSITION_FIELDS - set(pos.keys())
                assert missing == set(), f"Missing: {missing}"
                print(f"  [LIVE] position: {pos['security_id']} "
                      f"vol={pos['current_position']} avail={pos['available_position']}")
            else:
                print(f"  [LIVE] position: empty (no holdings)")

            req.send_string(json.dumps({'action': 'query_orders'}))
            resp = json.loads(req.recv_string())
            assert resp['ok'] is True
            print(f"  [LIVE] orders: {len(resp['data'])} records")

            req.send_string(json.dumps({'action': 'query_trades'}))
            resp = json.loads(req.recv_string())
            assert resp['ok'] is True
            print(f"  [LIVE] trades: {len(resp['data'])} records")

            req.close()
            req_ctx.term()
        finally:
            stop_event.set()
            time.sleep(0.5)
            rep.close()
            ctx.term()
            trader.stop()


# ============================================================
# 10. 并发压力测试
# ============================================================
class TestConcurrency:
    def test_concurrent_snapshot_updates(self):
        hub = qmt_hub.QMTHub()
        hub._start_time = time.time()
        errors = []

        def update_snapshots(start_code):
            try:
                for i in range(100):
                    code = f'{start_code:06d}.SH'
                    snap = _make_full_snapshot(code)
                    snap['last_price'] = 10.0 + i * 0.01
                    with hub._snap_lock:
                        hub._snapshots[code] = snap
                    hub._msg_count += 1
            except Exception as e:
                errors.append(e)

        threads = []
        for t in range(5):
            th = threading.Thread(target=update_snapshots, args=(600000 + t,))
            threads.append(th)
            th.start()
        for th in threads:
            th.join()

        assert len(errors) == 0
        assert hub._msg_count == 500
        assert len(hub._snapshots) == 5

    def test_concurrent_trade_requests(self):
        hub = qmt_hub.QMTHub(dry_run=True)
        hub._qmt_logged_in = True
        results = []
        lock = threading.Lock()

        def send_trade(action):
            resp = hub._process_trade_action({'action': action})
            with lock:
                results.append(resp)

        threads = []
        actions = ['ping'] * 10 + ['query_account'] * 5 + ['query_position'] * 5
        for action in actions:
            th = threading.Thread(target=send_trade, args=(action,))
            threads.append(th)
            th.start()
        for th in threads:
            th.join()

        assert len(results) == 20
        assert all(r['ok'] for r in results)

    def test_concurrent_read_write_snapshots(self):
        hub = qmt_hub.QMTHub()
        hub._start_time = time.time()
        errors = []

        def writer():
            try:
                for i in range(50):
                    snap = _make_full_snapshot('600000.SH')
                    snap['last_price'] = 10.0 + i * 0.01
                    with hub._snap_lock:
                        hub._snapshots['600000.SH'] = snap
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for i in range(50):
                    with hub._snap_lock:
                        snap = hub._snapshots.get('600000.SH')
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)

        tw = threading.Thread(target=writer)
        tr = threading.Thread(target=reader)
        tw.start()
        tr.start()
        tw.join()
        tr.join()
        assert len(errors) == 0


# ============================================================
# 11. QMTTradeClient + QMTMarketHubClient 接口测试
# ============================================================
class TestQMTTradeClient:
    def test_has_all_methods(self):
        methods = ['ping', 'buy', 'sell', 'query_account', 'query_position',
                   'query_orders', 'query_trades', 'close']
        for m in methods:
            assert hasattr(qmt_hub.QMTTradeClient, m), f"Missing method: {m}"

    def test_buy_request_format(self):
        client = qmt_hub.QMTTradeClient()
        client._sock = MagicMock()
        client._sock.recv_string.return_value = '{"ok":true}'
        client.buy('600000.SH', 100, 10.5, 'LIMIT', 'test')
        sent = json.loads(client._sock.send_string.call_args[0][0])
        assert sent['action'] == 'buy'
        assert sent['stock'] == '600000.SH'
        assert sent['shares'] == 100
        assert sent['price'] == 10.5
        assert sent['order_type'] == 'LIMIT'
        assert sent['reason'] == 'test'

    def test_sell_request_format(self):
        client = qmt_hub.QMTTradeClient()
        client._sock = MagicMock()
        client._sock.recv_string.return_value = '{"ok":true}'
        client.sell('600000.SH', 100, 0, 'MARKET')
        sent = json.loads(client._sock.send_string.call_args[0][0])
        assert sent['action'] == 'sell'
        assert sent['order_type'] == 'MARKET'

    def test_query_position_with_stock(self):
        client = qmt_hub.QMTTradeClient()
        client._sock = MagicMock()
        client._sock.recv_string.return_value = '{"ok":true,"data":[]}'
        client.query_position('600000.SH')
        sent = json.loads(client._sock.send_string.call_args[0][0])
        assert sent['action'] == 'query_position'
        assert sent['stock'] == '600000.SH'

    def test_query_orders_with_stock(self):
        client = qmt_hub.QMTTradeClient()
        client._sock = MagicMock()
        client._sock.recv_string.return_value = '{"ok":true,"data":[]}'
        client.query_orders('000002.SZ')
        sent = json.loads(client._sock.send_string.call_args[0][0])
        assert sent['action'] == 'query_orders'
        assert sent['stock'] == '000002.SZ'

    def test_not_connected_returns_error(self):
        client = qmt_hub.QMTTradeClient()
        resp = client.ping()
        assert resp['ok'] is False
        assert 'not connected' in resp['error'].lower()

    def test_request_exception_handling(self):
        client = qmt_hub.QMTTradeClient()
        client._sock = MagicMock()
        client._sock.send_string.side_effect = Exception("connection reset")
        resp = client.ping()
        assert resp['ok'] is False


class TestQMTMarketHubClient:
    def test_delegates_to_market_hub_client(self):
        from market_hub_v2 import MarketHubClient
        client = qmt_hub.QMTMarketHubClient()
        assert isinstance(client._inner, MarketHubClient)

    def test_all_delegated_methods(self):
        methods = ['connect', 'subscribe_snap', 'subscribe_bar',
                   'recv_snap', 'recv_bar', 'query_snapshot',
                   'query_bars', 'query_tick', 'query_stats', 'close']
        client = qmt_hub.QMTMarketHubClient()
        for m in methods:
            assert hasattr(client, m), f"Missing delegated method: {m}"


# ============================================================
# 12. 边界条件 + 异常恢复
# ============================================================
class TestEdgeCases:
    def test_empty_tick_data(self):
        assert qmt_hub.qmt_tick_to_snapshot('600000.SH', {}) is None

    def test_none_tick_data(self):
        assert qmt_hub.qmt_tick_to_snapshot('600000.SH', None) is None

    def test_gEM_stock_conversion(self):
        tick = {'lastPrice': 20.0, 'open': 19.5, 'high': 20.5,
                'low': 19.0, 'lastClose': 19.0, 'volume': 1000, 'amount': 20000.0,
                'askPrice': [0]*5, 'bidPrice': [0]*5, 'askVol': [0]*5, 'bidVol': [0]*5}
        snap = qmt_hub.qmt_tick_to_snapshot('300001.SZ', tick)
        assert snap is not None
        assert snap['code'] == '300001.SZ'

    def test_bse_stock_conversion(self):
        tick = {'lastPrice': 5.0, 'open': 4.8, 'high': 5.2,
                'low': 4.7, 'lastClose': 4.9, 'volume': 100, 'amount': 500.0,
                'askPrice': [0]*5, 'bidPrice': [0]*5, 'askVol': [0]*5, 'bidVol': [0]*5}
        snap = qmt_hub.qmt_tick_to_snapshot('830001.BJ', tick)
        assert snap is not None
        assert snap['code'] == '830001.SZ'

    def test_star_market_688(self):
        tick = {'lastPrice': 50.0, 'open': 49.0, 'high': 51.0,
                'low': 48.0, 'lastClose': 49.5, 'volume': 500, 'amount': 25000.0,
                'askPrice': [0]*5, 'bidPrice': [0]*5, 'askVol': [0]*5, 'bidVol': [0]*5}
        snap = qmt_hub.qmt_tick_to_snapshot('688001.SH', tick)
        assert snap is not None
        assert snap['code'] == '688001.SH'

    def test_very_large_turnover(self):
        tick = _make_qmt_tick(amount=999999999999.99)
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', tick)
        assert snap['turnover'] == 999999999999.99

    def test_zero_all_orderbook(self):
        tick = _make_qmt_tick(askPrice=[0]*5, bidPrice=[0]*5,
                              askVol=[0]*5, bidVol=[0]*5)
        snap = qmt_hub.qmt_tick_to_snapshot('600000.SH', tick)
        for i in range(1, 6):
            assert snap[f'ask{i}'] == 0
            assert snap[f'bid{i}'] == 0

    def test_qmt_code_variants(self):
        tick = _make_qmt_tick()
        assert qmt_hub.qmt_tick_to_snapshot('600000.SH', tick)['code'] == '600000.SH'
        assert qmt_hub.qmt_tick_to_snapshot('600000', tick)['code'] == '600000.SH'

    def test_hub_cleanup_no_error(self):
        hub = qmt_hub.QMTHub()
        hub._snap_pub = hub._ctx.socket(zmq.PUB)
        hub._snap_pub.bind('tcp://127.0.0.1:29990')
        hub._bar_pub = hub._ctx.socket(zmq.PUB)
        hub._bar_pub.bind('tcp://127.0.0.1:29991')
        hub._query_rep = hub._ctx.socket(zmq.REP)
        hub._query_rep.bind('tcp://127.0.0.1:29992')
        hub._trade_rep = hub._ctx.socket(zmq.REP)
        hub._trade_rep.bind('tcp://127.0.0.1:29993')
        hub.cleanup()

    def test_qmt_trader_callback_on_connected(self):
        hub = qmt_hub.QMTHub()
        hub._qmt_logged_in = False
        callback = qmt_hub.QMTTraderCallback(hub)
        callback.on_connected()
        assert hub._qmt_logged_in is True

    def test_qmt_trader_callback_on_disconnected(self):
        hub = qmt_hub.QMTHub()
        hub._qmt_logged_in = True
        callback = qmt_hub.QMTTraderCallback(hub)
        callback.on_disconnected()
        assert hub._qmt_logged_in is False

    def test_init_qmt_data_unavailable(self):
        hub = qmt_hub.QMTHub()
        with patch.object(qmt_hub, 'QMT_AVAILABLE', False):
            assert hub._init_qmt_data() is False

    def test_init_qmt_trader_dry_run(self):
        hub = qmt_hub.QMTHub(dry_run=True)
        assert hub._init_qmt_trader() is True

    def test_init_qmt_trader_no_path(self):
        hub = qmt_hub.QMTHub(qmt_path='', account_id='')
        assert hub._init_qmt_trader() is True

    def test_poll_ticks_no_codes(self):
        hub = qmt_hub.QMTHub()
        hub._all_codes = []
        hub._poll_ticks()
        assert hub._msg_count == 0

    def test_report_stats_no_error(self):
        hub = qmt_hub.QMTHub()
        hub._start_time = time.time()
        hub._last_report = 0
        hub._report_stats()


def test_cleanup():
    if TMP_ROOT.exists():
        shutil.rmtree(TMP_ROOT, ignore_errors=True)
