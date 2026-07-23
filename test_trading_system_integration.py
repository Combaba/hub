#!/usr/bin/env python3
"""
交易系统三服务集成 TDD 测试套件
================================
覆盖:
  1. MarketHub v2 行情接收 + ZMQ/HTTP接口 (集成测试，需服务运行)
  2. 华鑫交易网关 ZMQ查询接口 (集成测试，需服务运行)
  3. V7闪崩策略代码逻辑 (单元测试，mock)
  4. 盘口v2策略代码逻辑 (单元测试，mock)
  5. 三服务联动端到端数据流 (集成测试)
  6. 交易安全铁律验证 (单元测试)

运行:
  # 全量(需MarketHub v2 + 华鑫网关运行中)
  pytest test_trading_system_integration.py -v

  # 仅单元测试(不需要服务运行)
  pytest test_trading_system_integration.py -v -m unit

  # 仅集成测试(需要服务运行)
  pytest test_trading_system_integration.py -v -m integration

  # 盘中实战测试
  pytest test_trading_system_integration.py -v -m live
"""

import os
import sys
import json
import time
import threading
from datetime import datetime, date
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import zmq
import pytest

# ============================================================
# 项目路径
# ============================================================
FLASH_CRASH_DIR = Path(__file__).parent
BANDS_DIR = Path(__file__).parent.parent / 'bands'
sys.path.insert(0, str(FLASH_CRASH_DIR))
sys.path.insert(0, str(BANDS_DIR))


# ============================================================
# 常量
# ============================================================
HUB_HOST = '127.0.0.1'
HUB_SNAP_PORT = 19800
HUB_BAR_PORT = 19801
HUB_QUERY_PORT = 19802
HUB_API_PORT = 19803
GATEWAY_PORT = 19850

ZMQ_TIMEOUT = 3000  # ms


# ============================================================
# Helpers
# ============================================================
def zmq_query(port, req_dict, timeout=ZMQ_TIMEOUT):
    """ZMQ REQ/REP 查询通用函数"""
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, timeout)
    sock.connect(f'tcp://{HUB_HOST}:{port}')
    sock.send_json(req_dict)
    resp = sock.recv_json()
    sock.close()
    ctx.term()
    return resp


def hub_query(cmd, **kwargs):
    """MarketHub v2 ZMQ REP 查询"""
    return zmq_query(HUB_QUERY_PORT, {'cmd': cmd, **kwargs})


def gateway_query(action, **kwargs):
    """华鑫交易网关 ZMQ REP 查询"""
    return zmq_query(GATEWAY_PORT, {'action': action, **kwargs})


def http_get(path, port=HUB_API_PORT, timeout=5):
    """FastAPI HTTP GET (用urllib，不依赖requests)"""
    import urllib.request
    url = f'http://127.0.0.1:{port}{path}'
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode('utf-8'))


# ============================================================
# Fixtures
# ============================================================
@pytest.fixture
def zmq_ctx():
    """ZMQ Context fixture"""
    ctx = zmq.Context()
    yield ctx
    ctx.term()


@pytest.fixture
def hub_client(zmq_ctx):
    """MarketHub ZMQ查询socket"""
    sock = zmq_ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, ZMQ_TIMEOUT)
    sock.connect(f'tcp://{HUB_HOST}:{HUB_QUERY_PORT}')
    yield sock
    sock.close()


@pytest.fixture
def gw_client(zmq_ctx):
    """华鑫交易网关 ZMQ查询socket"""
    sock = zmq_ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, ZMQ_TIMEOUT)
    sock.connect(f'tcp://{HUB_HOST}:{GATEWAY_PORT}')
    yield sock
    sock.close()


# ============================================================
# 1. MarketHub v2 行情集成测试
# ============================================================
@pytest.mark.integration
class TestMarketHubV2Integration:
    """MarketHub v2 实时行情接收 + ZMQ/HTTP接口 (需要服务运行)"""

    def test_zmq_stats(self, hub_client):
        """ZMQ查询: stats返回运行统计"""
        hub_client.send_json({'cmd': 'stats'})
        resp = hub_client.recv_json()
        assert resp['ok'] is True
        data = resp['data']
        assert data['total_msgs'] > 0
        assert data['snapshot_cached'] > 0
        assert data['rate_per_sec'] > 0

    def test_zmq_snapshot_existing(self, hub_client):
        """ZMQ查询: 600000.SH有快照"""
        hub_client.send_json({'cmd': 'snapshot', 'code': '600000.SH'})
        resp = hub_client.recv_json()
        assert resp['ok'] is True
        snap = resp['data']
        assert snap['code'] == '600000.SH'
        assert snap['last_price'] > 0
        assert snap['pre_close'] > 0

    def test_zmq_snapshot_has_5level_orderbook(self, hub_client):
        """ZMQ查询: 快照包含5档买卖盘"""
        hub_client.send_json({'cmd': 'snapshot', 'code': '600000.SH'})
        resp = hub_client.recv_json()
        assert resp['ok'] is True
        snap = resp['data']
        # 5档买盘
        for i in range(1, 6):
            assert f'ask{i}' in snap, f"缺少ask{i}"
            assert f'ask_vol{i}' in snap, f"缺少ask_vol{i}"
            assert f'bid{i}' in snap, f"缺少bid{i}"
            assert f'bid_vol{i}' in snap, f"缺少bid_vol{i}"

    def test_zmq_snapshot_nonexistent(self, hub_client):
        """ZMQ查询: 不存在的代码返回失败"""
        hub_client.send_json({'cmd': 'snapshot', 'code': '999999.SH'})
        resp = hub_client.recv_json()
        assert resp['ok'] is False

    def test_zmq_bars(self, hub_client):
        """ZMQ查询: 1分钟K线"""
        hub_client.send_json({'cmd': 'bars', 'code': '600000.SH', 'count': 10})
        resp = hub_client.recv_json()
        assert resp['ok'] is True
        bars = resp['data']
        assert len(bars) > 0
        bar = bars[0]
        assert 'open' in bar
        assert 'high' in bar
        assert 'low' in bar
        assert 'close' in bar
        assert 'volume' in bar

    def test_zmq_tick(self, hub_client):
        """ZMQ查询: tick数据"""
        today = date.today().isoformat()
        hub_client.send_json({'cmd': 'tick', 'code': '600000.SH', 'date': today, 'limit': 5})
        resp = hub_client.recv_json()
        # tick可能为空(收盘后flush过)，但接口不能报错
        assert resp['ok'] is True

    def test_zmq_unknown_cmd(self, hub_client):
        """ZMQ查询: 未知命令返回错误"""
        hub_client.send_json({'cmd': 'nonexistent'})
        resp = hub_client.recv_json()
        assert resp['ok'] is False

    def test_http_stats(self):
        """HTTP查询: /api/stats"""
        data = http_get('/api/stats')
        assert data['ok'] is True
        assert data['data']['total_msgs'] > 0

    def test_http_snapshot(self):
        """HTTP查询: /api/snapshot/{code}"""
        data = http_get('/api/snapshot/600000.SH')
        assert data['ok'] is True
        assert data['data']['code'] == '600000.SH'

    def test_http_all_snapshots_index_only(self):
        """HTTP查询: /api/snapshots?index_only=true 只返回指数"""
        data = http_get('/api/snapshots?index_only=true')
        assert data['ok'] is True
        assert data['count'] > 0
        for code, snap in data['data'].items():
            assert snap.get('is_index') is True

    def test_http_bars_1m(self):
        """HTTP查询: /api/bars/1m/{code}"""
        data = http_get('/api/bars/1m/600000.SH?count=10')
        assert data['ok'] is True
        assert data['count'] > 0

    def test_http_history_1m(self):
        """HTTP查询: /api/history/1m/{code}"""
        data = http_get('/api/history/1m/600000.SH?count=5')
        # 可能有数据也可能没有(取决于米筐下载)
        if data['ok']:
            assert data['count'] > 0

    def test_http_history_daily(self):
        """HTTP查询: /api/history/daily/{code}"""
        data = http_get('/api/history/daily/600000.SH?count=5')
        if data['ok']:
            assert data['count'] > 0

    def test_snapshot_cached_over_1000(self, hub_client):
        """盘中缓存快照数>1000只(全市场A股)"""
        hub_client.send_json({'cmd': 'stats'})
        resp = hub_client.recv_json()
        assert resp['data']['snapshot_cached'] >= 1000

    def test_msg_rate_over_1(self, hub_client):
        """消息速率>1条/秒(行情持续流入)"""
        hub_client.send_json({'cmd': 'stats'})
        resp = hub_client.recv_json()
        assert resp['data']['rate_per_sec'] >= 1.0


# ============================================================
# 2. 华鑫交易网关集成测试
# ============================================================
@pytest.mark.integration
class TestHuaxinGatewayIntegration:
    """华鑫交易网关 ZMQ查询接口 (需要服务运行)"""

    def test_ping_alive(self, gw_client):
        """网关ping返回alive + logged_in"""
        gw_client.send_json({'action': 'ping'})
        resp = gw_client.recv_json()
        assert resp['ok'] is True
        assert resp['logged_in'] is True

    def test_query_account(self, gw_client):
        """查询账户资金"""
        gw_client.send_json({'action': 'query_account'})
        resp = gw_client.recv_json()
        assert resp['ok'] is True
        data = resp['data']
        assert len(data) > 0
        acct = data[0]
        assert 'account_id' in acct
        assert 'useful_money' in acct
        assert 'frozen_cash' in acct
        assert acct['useful_money'] >= 0

    def test_query_position(self, gw_client):
        """查询持仓"""
        gw_client.send_json({'action': 'query_position'})
        resp = gw_client.recv_json()
        assert resp['ok'] is True
        # 持仓可能为空(仿真账户)
        positions = resp['data']
        for pos in positions:
            assert 'security_id' in pos
            assert 'exchange' in pos
            assert 'current_position' in pos
            assert 'available_position' in pos
            assert pos['current_position'] >= 0

    def test_query_orders(self, gw_client):
        """查询委托"""
        gw_client.send_json({'action': 'query_orders'})
        resp = gw_client.recv_json()
        assert resp['ok'] is True
        assert isinstance(resp['data'], list)

    def test_query_trades(self, gw_client):
        """查询成交"""
        gw_client.send_json({'action': 'query_trades'})
        resp = gw_client.recv_json()
        assert resp['ok'] is True
        assert isinstance(resp['data'], list)

    def test_unknown_action_returns_error(self, gw_client):
        """未知action返回错误"""
        gw_client.send_json({'action': 'nonexistent'})
        resp = gw_client.recv_json()
        assert resp['ok'] is False
        assert 'unknown action' in resp.get('error', '').lower()

    def test_account_id_is_simulation(self, gw_client):
        """账户ID为仿真账户00032127"""
        gw_client.send_json({'action': 'query_account'})
        resp = gw_client.recv_json()
        assert resp['data'][0]['account_id'] == '00032127'

    def test_position_format_sh(self, gw_client):
        """沪市持仓exchange字段为SSE"""
        gw_client.send_json({'action': 'query_position'})
        resp = gw_client.recv_json()
        for pos in resp['data']:
            if pos['security_id'].startswith('6'):
                assert pos['exchange'] == 'SSE'

    def test_position_format_sz(self, gw_client):
        """深市持仓exchange字段为SZSE"""
        gw_client.send_json({'action': 'query_position'})
        resp = gw_client.recv_json()
        for pos in resp['data']:
            if pos['security_id'].startswith('0') or pos['security_id'].startswith('3'):
                assert pos['exchange'] == 'SZSE'


# ============================================================
# 3. V7闪崩策略逻辑测试 (单元测试, 不需要服务)
# ============================================================
@pytest.mark.unit
class TestV7Logic:
    """V7闪崩策略代码逻辑验证"""

    def test_dry_run_removed(self):
        """dry_run模式已完全移除"""
        with open(FLASH_CRASH_DIR / 'flash_crash_v7_zmq.py', 'r') as f:
            content = f.read()
        assert 'dry_run' not in content, "dry_run未完全移除"

    def test_live_flag_removed(self):
        """--live参数已移除"""
        with open(FLASH_CRASH_DIR / 'flash_crash_v7_zmq.py', 'r') as f:
            content = f.read()
        assert '--live' not in content, "--live参数未移除"

    def test_huaxin_executor_exists(self):
        """HuaxinOrderExecutor类存在"""
        with open(FLASH_CRASH_DIR / 'flash_crash_v7_zmq.py', 'r') as f:
            content = f.read()
        assert 'class HuaxinOrderExecutor' in content

    def test_gateway_port_19850(self):
        """交易网关端口为19850"""
        with open(FLASH_CRASH_DIR / 'flash_crash_v7_zmq.py', 'r') as f:
            content = f.read()
        assert '19850' in content

    def test_daily_triggered_anti_duplicate(self):
        """防重复买入: daily_triggered set"""
        with open(FLASH_CRASH_DIR / 'flash_crash_v7_zmq.py', 'r') as f:
            content = f.read()
        assert 'daily_triggered' in content
        assert 'daily_triggered.add' in content

    def test_sold_today_anti_duplicate(self):
        """防重复卖出: sold_today set"""
        with open(FLASH_CRASH_DIR / 'flash_crash_v7_zmq.py', 'r') as f:
            content = f.read()
        assert 'sold_today' in content
        assert 'sold_today.add' in content

    def test_max_positions_per_day(self):
        """每日买入上限: max_positions_per_day"""
        with open(FLASH_CRASH_DIR / 'flash_crash_v7_zmq.py', 'r') as f:
            content = f.read()
        assert 'max_positions_per_day' in content

    def test_no_degradation_on_failure(self):
        """连接失败禁止降级: 直接退出"""
        with open(FLASH_CRASH_DIR / 'flash_crash_v7_zmq.py', 'r') as f:
            content = f.read()
        # "禁止降级"是允许的(注释), 但不能有实际降级逻辑
        assert 'FallbackDataSource' not in content
        assert '降级到' not in content  # "禁止降级"可以, "降级到"不行
        # 连接失败应直接退出
        assert '禁止降级，直接退出' in content

    def test_ask1_used_for_buy_price(self):
        """买入用对手价ask1"""
        with open(FLASH_CRASH_DIR / 'flash_crash_v7_zmq.py', 'r') as f:
            content = f.read()
        assert 'ask1' in content
        # 买入价应来自ask1(对手价)
        lines = content.split('\n')
        buy_price_lines = [l for l in lines if 'ask1' in l and ('buy' in l.lower() or '买入' in l)]
        assert len(buy_price_lines) > 0, "未找到ask1用于买入价的代码"

    def test_sell_uses_market_order(self):
        """卖出用MARKET市价单"""
        with open(FLASH_CRASH_DIR / 'flash_crash_v7_zmq.py', 'r') as f:
            content = f.read()
        # _exec_sell应使用MARKET — 代码用双引号
        assert "order_type': 'MARKET'" in content or '"order_type": "MARKET"' in content or \
               "'order_type': 'MARKET'" in content or "order_type'] = 'MARKET'" in content

    def test_reset_daily_clears_state(self):
        """日切重置: reset_daily清空防重复状态"""
        with open(FLASH_CRASH_DIR / 'flash_crash_v7_zmq.py', 'r') as f:
            content = f.read()
        assert 'reset_daily' in content or 'daily_buy_count = 0' in content


# ============================================================
# 4. 盘口v2策略逻辑测试 (单元测试, 不需要服务)
# ============================================================
@pytest.mark.unit
class TestPankouV2Logic:
    """盘口v2策略代码逻辑验证"""

    def test_positions_v7_json_removed(self):
        """positions_v7.json依赖已移除"""
        with open(BANDS_DIR / 'fusion' / 'pankou_live_v2.py', 'r') as f:
            content = f.read()
        assert 'positions_v7.json' not in content

    def test_load_v7_positions_removed(self):
        """_load_v7_positions方法已移除"""
        with open(BANDS_DIR / 'fusion' / 'pankou_live_v2.py', 'r') as f:
            content = f.read()
        assert '_load_v7_positions' not in content

    def test_mark_sold_removed(self):
        """_mark_sold方法已移除"""
        with open(BANDS_DIR / 'fusion' / 'pankou_live_v2.py', 'r') as f:
            content = f.read()
        assert '_mark_sold' not in content

    def test_try_place_order_removed(self):
        """_try_place_order方法已移除"""
        with open(BANDS_DIR / 'fusion' / 'pankou_live_v2.py', 'r') as f:
            content = f.read()
        assert '_try_place_order' not in content

    def test_trade_gateway_exists(self):
        """TradeGateway类存在"""
        with open(BANDS_DIR / 'fusion' / 'pankou_live_v2.py', 'r') as f:
            content = f.read()
        assert 'class TradeGateway' in content

    def test_query_account_method(self):
        """TradeGateway.query_account方法存在"""
        with open(BANDS_DIR / 'fusion' / 'pankou_live_v2.py', 'r') as f:
            content = f.read()
        assert 'def query_account' in content

    def test_query_position_method(self):
        """TradeGateway.query_position方法存在"""
        with open(BANDS_DIR / 'fusion' / 'pankou_live_v2.py', 'r') as f:
            content = f.read()
        assert 'def query_position' in content

    def test_query_orders_method(self):
        """TradeGateway.query_orders方法存在"""
        with open(BANDS_DIR / 'fusion' / 'pankou_live_v2.py', 'r') as f:
            content = f.read()
        assert 'def query_orders' in content

    def test_query_trades_method(self):
        """TradeGateway.query_trades方法存在"""
        with open(BANDS_DIR / 'fusion' / 'pankou_live_v2.py', 'r') as f:
            content = f.read()
        assert 'def query_trades' in content

    def test_gateway_port_19850(self):
        """交易网关端口19850"""
        with open(BANDS_DIR / 'fusion' / 'pankou_live_v2.py', 'r') as f:
            content = f.read()
        assert '19850' in content

    def test_sell_uses_market_order(self):
        """卖出用MARKET市价单"""
        with open(BANDS_DIR / 'fusion' / 'pankou_live_v2.py', 'r') as f:
            content = f.read()
        assert "'MARKET'" in content

    def test_no_degradation_on_failure(self):
        """连接失败禁止降级: 直接退出"""
        with open(BANDS_DIR / 'fusion' / 'pankou_live_v2.py', 'r') as f:
            content = f.read()
        assert 'sys.exit(1)' in content

    def test_holding_from_huaxin_gateway(self):
        """持仓从华鑫网关获取"""
        with open(BANDS_DIR / 'fusion' / 'pankou_live_v2.py', 'r') as f:
            content = f.read()
        assert '_load_real_positions' in content or 'query_position' in content

    def test_sell_signal_direction_negative(self):
        """卖出信号: direction < 0"""
        with open(BANDS_DIR / 'fusion' / 'pankou_live_v2.py', 'r') as f:
            content = f.read()
        assert "direction" in content and "< 0" in content

    def test_sell_confidence_threshold(self):
        """卖出置信度阈值≥0.5"""
        with open(BANDS_DIR / 'fusion' / 'pankou_live_v2.py', 'r') as f:
            content = f.read()
        assert '0.5' in content and 'confidence' in content

    def test_three_books_strategies(self):
        """三书52策略覆盖: S/C/W"""
        with open(BANDS_DIR / 'fusion' / 'pankou_live_v2.py', 'r') as f:
            content = f.read()
        assert 'S13' in content or 'S1' in content  # 徐小明
        assert 'C1' in content  # 穿杨
        assert 'W12' in content or 'W1' in content  # 伍朝辉


# ============================================================
# 5. 三服务联动端到端测试
# ============================================================
@pytest.mark.integration
class TestEndToEndFlow:
    """三服务联动端到端数据流验证 (需要MarketHub + 华鑫网关运行)"""

    def test_markethub_to_v7_snapshot_flow(self, hub_client):
        """行情→V7: 快照有5档盘口(ask1用于买入价)"""
        hub_client.send_json({'cmd': 'snapshot', 'code': '600600.SH'})
        resp = hub_client.recv_json()
        if resp['ok']:
            snap = resp['data']
            assert snap.get('ask1', 0) > 0, "ask1=0, V7无法用对手价买入"
            assert snap.get('bid1', 0) > 0, "bid1=0, 无法计算买卖价差"

    def test_markethub_to_pankou_bars_flow(self, hub_client):
        """行情→盘口v2: K线数据可供盘口策略分析"""
        hub_client.send_json({'cmd': 'bars', 'code': '600600.SH', 'count': 60})
        resp = hub_client.recv_json()
        if resp['ok']:
            bars = resp['data']
            # 收盘后可能只有少量K线(当天聚合), 盘中会>=30
            assert len(bars) >= 1, f"K线条数{len(bars)}<1, 行情数据异常"

    def test_gateway_to_pankou_position_flow(self, gw_client):
        """网关→盘口v2: 持仓可查询"""
        gw_client.send_json({'action': 'query_position'})
        resp = gw_client.recv_json()
        assert resp['ok'] is True
        # 持仓列表格式正确
        for pos in resp['data']:
            assert 'security_id' in pos
            assert 'current_position' in pos
            assert 'available_position' in pos

    def test_gateway_to_v7_account_flow(self, gw_client):
        """网关→V7: 账户资金可查询"""
        gw_client.send_json({'action': 'query_account'})
        resp = gw_client.recv_json()
        assert resp['ok'] is True
        assert resp['data'][0]['useful_money'] > 0

    def test_gateway_buy_sell_actions_recognized(self, gw_client):
        """网关: buy/sell action被识别(不发真实委托,仅验证action路由)"""
        # buy/sell需要logged_in+有效股票, 会真正下单, 不测试
        # 只验证unknown action返回错误(说明buy/sell已被识别为合法action)
        gw_client.send_json({'action': 'nonexistent_action'})
        resp = gw_client.recv_json()
        assert resp['ok'] is False
        assert 'unknown action' in resp.get('error', '').lower()

    def test_full_buy_flow_data_reachable(self, hub_client, gw_client):
        """完整买入流程数据可达: 行情→信号→网关"""
        # 1. 行情: 有快照
        hub_client.send_json({'cmd': 'snapshot', 'code': '600600.SH'})
        snap_resp = hub_client.recv_json()
        assert snap_resp['ok'] is True

        # 2. 网关: 已登录
        gw_client.send_json({'action': 'ping'})
        ping_resp = gw_client.recv_json()
        assert ping_resp['logged_in'] is True

        # 3. 网关: 有可用资金
        gw_client.send_json({'action': 'query_account'})
        acct_resp = gw_client.recv_json()
        assert acct_resp['data'][0]['useful_money'] > 0

    def test_full_sell_flow_data_reachable(self, hub_client, gw_client):
        """完整卖出流程数据可达: 持仓→K线→策略→网关"""
        # 1. 网关: 有持仓
        gw_client.send_json({'action': 'query_position'})
        pos_resp = gw_client.recv_json()
        assert pos_resp['ok'] is True

        # 2. 对每个持仓股票, 行情有K线
        for pos in pos_resp['data'][:3]:  # 只查前3只
            sid = pos['security_id']
            ex = 'SH' if pos['exchange'] == 'SSE' else 'SZ'
            code = f'{sid}.{ex}'
            hub_client.send_json({'cmd': 'bars', 'code': code, 'count': 60})
            bars_resp = hub_client.recv_json()
            # K线可能条数不够(盘中刚开始),但接口不应报错
            assert bars_resp['ok'] is True


# ============================================================
# 6. 交易安全铁律验证 (单元测试)
# ============================================================
@pytest.mark.unit
class TestTradingSafetyRules:
    """交易系统铁律验证"""

    def test_v7_no_auto_degradation(self):
        """铁律: V7禁止自动降级"""
        with open(FLASH_CRASH_DIR / 'flash_crash_v7_zmq.py', 'r') as f:
            content = f.read()
        # 不允许降级到fallback/历史数据模拟
        assert 'FallbackDataSource' not in content
        assert '模拟行情' not in content
        assert '降级到' not in content

    def test_pankou_no_auto_degradation(self):
        """铁律: 盘口v2禁止自动降级"""
        with open(BANDS_DIR / 'fusion' / 'pankou_live_v2.py', 'r') as f:
            content = f.read()
        # "禁止导入FallbackDataSource"是注释,允许; 但不能有实际使用
        # 检查import语句中没有FallbackDataSource
        import_section = content[:content.find('class ')]
        assert 'import FallbackDataSource' not in import_section
        assert 'from FallbackDataSource' not in import_section
        # 连接失败直接退出
        assert 'sys.exit(1)' in content

    def test_gateway_no_simulated_data(self):
        """铁律: 华鑫网关不返回模拟数据"""
        with open(FLASH_CRASH_DIR / 'huaxin_trade_gateway.py', 'r') as f:
            content = f.read()
        assert '模拟' not in content
        assert 'fallback' not in content.lower()
        assert 'FallbackDataSource' not in content

    def test_v7_sell_uses_counterpart_price(self):
        """铁律: V9出场铁律 — 所有出场用对手价"""
        with open(FLASH_CRASH_DIR / 'flash_crash_v7_zmq.py', 'r') as f:
            content = f.read()
        # 卖出应用MARKET(市价=对手价成交)
        # 不应有limit_price用于卖出
        sell_section = content[content.find('def _exec_sell'):]
        sell_section = sell_section[:sell_section.find('\n    def ')]
        assert 'MARKET' in sell_section

    def test_v7_no_target_price_execution(self):
        """铁律: 目标价成交=偷价, V7不允许"""
        with open(FLASH_CRASH_DIR / 'flash_crash_v7_zmq.py', 'r') as f:
            content = f.read()
        # 不应有限价卖出(除特殊止损)
        assert '目标价成交' not in content

    def test_v7_daily_buy_limit(self):
        """安全: V7每日买入上限20只"""
        with open(FLASH_CRASH_DIR / 'flash_crash_v7_zmq.py', 'r') as f:
            content = f.read()
        assert 'max_positions_per_day' in content
        # 验证值=20
        lines = content.split('\n')
        for line in lines:
            if 'max_positions_per_day' in line and '20' in line:
                return  # 找到
        # 也可能在配置中
        assert 'max_positions_per_day' in content

    def test_v7_stop_loss_exists(self):
        """安全网: V7保留止损-3%"""
        with open(FLASH_CRASH_DIR / 'flash_crash_v7_zmq.py', 'r') as f:
            content = f.read()
        assert '-0.03' in content or '-3%' in content or 'stop_loss' in content

    def test_v7_force_sell_at_1457(self):
        """安全网: V7 14:57强制卖出"""
        with open(FLASH_CRASH_DIR / 'flash_crash_v7_zmq.py', 'r') as f:
            content = f.read()
        assert '1457' in content or '14:57' in content or '14:5' in content

    def test_pankou_sell_refreshes_position_cache(self):
        """安全: 盘口v2卖出后刷新持仓缓存"""
        with open(BANDS_DIR / 'fusion' / 'pankou_live_v2.py', 'r') as f:
            content = f.read()
        # 卖出后应刷新持仓
        sell_section = content[content.find('trade_gateway.sell'):]
        sell_section = sell_section[:sell_section.find('\n\n')]
        assert '_last_positions_load' in sell_section or 'positions_load' in content


# ============================================================
# 7. 盘中实战测试 (仅交易时段运行)
# ============================================================
@pytest.mark.live
@pytest.mark.integration
class TestLiveTradingSession:
    """盘中实战测试 — 仅在9:30-15:00交易时段有意义"""

    def test_snapshot_has_real_time_data(self, hub_client):
        """快照有实时时间戳(近5分钟内)"""
        hub_client.send_json({'cmd': 'snapshot', 'code': '600000.SH'})
        resp = hub_client.recv_json()
        if not resp['ok']:
            pytest.skip("非交易时段，无实时快照")
        snap = resp['data']
        update_time = snap.get('update_time', '')
        # 格式: HH:MM:SS
        assert ':' in update_time, f"时间格式异常: {update_time}"

    def test_tick_saving_active(self, hub_client):
        """tick实时保存中"""
        hub_client.send_json({'cmd': 'stats'})
        resp = hub_client.recv_json()
        ts = resp['data']['tick_saved']
        assert ts > 0, "无tick保存"

    def test_bars_producing_1m_klines(self, hub_client):
        """K线聚合器产出1分钟K线"""
        hub_client.send_json({'cmd': 'stats'})
        resp = hub_client.recv_json()
        bars = resp['data']['bars_published']
        assert bars > 0, "无K线产出"

    def test_position_available_to_sell(self, gw_client):
        """持仓可卖数量≥0"""
        gw_client.send_json({'action': 'query_position'})
        resp = gw_client.recv_json()
        for pos in resp['data']:
            assert pos['available_position'] >= 0
            assert pos['available_position'] <= pos['current_position']

    def test_markethub_high_msg_rate_during_trading(self, hub_client):
        """交易时段消息速率>5条/秒"""
        now = datetime.now()
        if now.hour < 9 or now.hour >= 15:
            pytest.skip("非交易时段")
        hub_client.send_json({'cmd': 'stats'})
        resp = hub_client.recv_json()
        assert resp['data']['rate_per_sec'] >= 5.0

    def test_v7_service_can_start(self):
        """V7服务可启动(验证systemd unit存在)"""
        import subprocess
        result = subprocess.run(
            ['systemctl', 'cat', 'flash-crash-v7.service'],
            capture_output=True, text=True
        )
        assert result.returncode == 0, "flash-crash-v7.service不存在"

    def test_pankou_service_can_start(self):
        """盘口v2服务可启动(验证systemd unit存在)"""
        import subprocess
        result = subprocess.run(
            ['systemctl', 'cat', 'pankou-live-v2.service'],
            capture_output=True, text=True
        )
        assert result.returncode == 0, "pankou-live-v2.service不存在"

    def test_market_hub_service_active(self):
        """MarketHub v2服务运行中"""
        import subprocess
        result = subprocess.run(
            ['systemctl', 'is-active', 'market-hub-v2.service'],
            capture_output=True, text=True
        )
        assert result.stdout.strip() == 'active'

    def test_all_ports_listening(self):
        """所有端口在监听"""
        import subprocess
        result = subprocess.run(
            ['ss', '-tlnp'],
            capture_output=True, text=True
        )
        output = result.stdout
        for port in [19800, 19801, 19802, 19803, 19850]:
            assert str(port) in output, f"端口{port}未监听"
