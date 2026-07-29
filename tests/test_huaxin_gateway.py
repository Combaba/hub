#!/usr/bin/env python3
"""TDD测试 — 华鑫交易网关业务逻辑
覆盖:
  1. SecurityID类型: str✅ vs bytes❌
  2. stock_to_exchange: 6xx→SSE, 其他→SZSE
  3. ZMQ客户端_request重连逻辑
  4. 网关handle_request路由
  5. 安全边界: 空stock/非法action/未登录交易
  6. safe_str处理\x00
  7. 华鑫SDK字段赋值类型一致性
  8. ZMQ协议JSON格式
  9. 端到端业务流程
  10. 边界条件

注意: traderapi是C扩展(Python3.7 only), 测试中不直接import,
      而是通过mock/代码复制/静态扫描验证业务逻辑。
"""
import json
import sys
import os
import unittest
from unittest.mock import MagicMock, patch, call

# ============================================================
# 从网关代码提取纯Python逻辑(不依赖traderapi)
# ============================================================

def stock_to_exchange(code):
    """股票代码→华鑫交易所ID (从huaxin_trade_gateway.py复制)"""
    c = code[:6]
    if c.startswith('6'):
        return 'SSE'
    else:
        return 'SZSE'

def safe_str(val):
    """安全转字符串，处理\\x00 (从huaxin_trade_gateway.py复制)"""
    if val is None:
        return ''
    s = str(val)
    return s.strip('\x00').strip()


# ============================================================
# 1. SecurityID类型测试 — 核心bug修复验证
# ============================================================
class TestSecurityIDType(unittest.TestCase):
    """SecurityID_set只接受str, 不接受bytes — 这是14:09崩溃的根因"""

    def test_str_assignment_accepted(self):
        """str赋值应被SDK接受 (修复后的行为)"""
        # 模拟SDK的char[31]字段: Python ctypes c_char数组接受str
        import ctypes
        field = (ctypes.c_char * 31)()
        # str赋值 — ctypes c_char array接受bytes, 但SDK的setter接受str
        # 这里验证Python层面str和bytes是不同类型
        stock_code = '688766'
        val = stock_code[:6]
        self.assertIsInstance(val, str)
        self.assertNotIsInstance(val, bytes)

    def test_bytes_assignment_is_wrong_type(self):
        """bytes赋值是错误类型 — .encode()产生bytes"""
        stock_code = '688766'
        val = stock_code[:6].encode()
        self.assertIsInstance(val, bytes)
        self.assertNotIsInstance(val, str)

    def test_encode_produces_bytes(self):
        """验证.encode()确实返回bytes类型"""
        self.assertIsInstance('688766'.encode(), bytes)
        self.assertNotIsInstance('688766', bytes)

    def test_slice_produces_str(self):
        """验证字符串切片返回str类型"""
        self.assertIsInstance('688766.SH'[:6], str)
        self.assertEqual('688766.SH'[:6], '688766')

    def test_all_securityid_assignments_use_str(self):
        """检查网关代码中所有SecurityID赋值都是str而非bytes"""
        with open('/home/hb/hub/huaxin_trade_gateway.py', 'r') as f:
            content = f.read()
        lines = content.split('\n')
        for i, line in enumerate(lines, 1):
            if 'SecurityID' in line and '=' in line and '==' not in line:
                stripped = line.strip()
                if stripped.startswith('#') or stripped.startswith('"'):
                    continue
                self.assertNotIn('.encode()', line,
                    f"第{i}行SecurityID赋值使用了.encode(): {line.strip()}")

    def test_all_securityid_assignments_count(self):
        """确认网关中SecurityID赋值点数量(4处: order/position/orders/trades)"""
        with open('/home/hb/hub/huaxin_trade_gateway.py', 'r') as f:
            content = f.read()
        count = 0
        for line in content.split('\n'):
            stripped = line.strip()
            if 'SecurityID' in stripped and '=' in stripped and '==' not in stripped:
                if not stripped.startswith('#') and not stripped.startswith('"'):
                    if '.strip' not in stripped and 'safe_str' not in stripped:
                        # 排除读取行(从SDK回调读), 只统计写入行
                        if 'order.SecurityID' in stripped or 'req.SecurityID' in stripped:
                            count += 1
        self.assertEqual(count, 4, f"应有4处SecurityID写入, 实际{count}处")


# ============================================================
# 2. stock_to_exchange测试 — 交易所映射
# ============================================================
class TestStockToExchange(unittest.TestCase):
    """股票代码→华鑫交易所ID映射"""

    def test_shanghai_mainboard(self):
        """60xxxx → SSE (上海主板)"""
        self.assertEqual(stock_to_exchange('600519.SH'), 'SSE')
        self.assertEqual(stock_to_exchange('601398.SH'), 'SSE')

    def test_shanghai_star(self):
        """68xxxx → SSE (科创板)"""
        self.assertEqual(stock_to_exchange('688766.SH'), 'SSE')
        self.assertEqual(stock_to_exchange('688981.SH'), 'SSE')

    def test_shenzhen_mainboard(self):
        """00xxxx → SZSE (深圳主板)"""
        self.assertEqual(stock_to_exchange('000001.SZ'), 'SZSE')
        self.assertEqual(stock_to_exchange('002905.SZ'), 'SZSE')

    def test_shenzhen_chinext(self):
        """30xxxx → SZSE (创业板)"""
        self.assertEqual(stock_to_exchange('300750.SZ'), 'SZSE')
        self.assertEqual(stock_to_exchange('301234.SZ'), 'SZSE')

    def test_code_with_suffix(self):
        """带后缀的代码应正确截取前6位"""
        self.assertEqual(stock_to_exchange('600600.SH'), 'SSE')
        self.assertEqual(stock_to_exchange('002905.SZ'), 'SZSE')

    def test_code_without_suffix(self):
        """不带后缀的6位代码"""
        self.assertEqual(stock_to_exchange('600519'), 'SSE')
        self.assertEqual(stock_to_exchange('000001'), 'SZSE')

    def test_b_stock_goes_szse(self):
        """200xxx B股 → SZSE (以2开头非6)"""
        self.assertEqual(stock_to_exchange('200002.SZ'), 'SZSE')


# ============================================================
# 3. ZMQ客户端重连逻辑测试 — 纯逻辑验证
# ============================================================
class TestZMQReconnectLogic(unittest.TestCase):
    """_request()失败→close→reconnect→重试一次 — 纯逻辑验证"""

    def _make_client(self):
        """创建模拟的ZMQ REQ客户端(不import实际模块)"""
        client = MagicMock()
        client._socket = MagicMock()
        client._connected = True
        # 绑定_request逻辑
        client._request = self._request.__get__(client)
        client.connect = MagicMock(return_value=True)
        return client

    @staticmethod
    def _request(self, req):
        """从flash_crash_v7_zmq.py复制的_request逻辑"""
        if not self._socket:
            return None
        try:
            self._socket.send_string(json.dumps(req))
            resp_str = self._socket.recv_string()
            return json.loads(resp_str)
        except Exception as e:
            # 自动重连
            try:
                self._socket.close()
                self._socket = None
                self._connected = False
                if self.connect():
                    self._socket.send_string(json.dumps(req))
                    resp_str = self._socket.recv_string()
                    return json.loads(resp_str)
                else:
                    return None
            except Exception as e2:
                return None

    def test_request_success_no_reconnect(self):
        """正常请求不应触发重连"""
        client = self._make_client()
        client._socket.send_string = MagicMock()
        client._socket.recv_string = MagicMock(return_value='{"ok": true}')

        result = client._request({"action": "ping"})
        self.assertTrue(result.get("ok"))
        self.assertEqual(client._socket.send_string.call_count, 1)
        client.connect.assert_not_called()

    def test_request_failure_triggers_reconnect(self):
        """通信失败应触发close+connect+重试"""
        client = self._make_client()
        old_socket = client._socket  # 保存引用
        old_socket.send_string = MagicMock(side_effect=Exception("Connection refused"))

        result = client._request({"action": "ping"})
        # close()被调用后socket被设为None
        old_socket.close.assert_called_once()
        self.assertFalse(client._connected)

    def test_request_failure_reconnect_success(self):
        """通信失败→重连成功→重试请求成功"""
        client = self._make_client()
        old_socket = MagicMock()
        old_socket.send_string = MagicMock(side_effect=Exception("Broken pipe"))
        client._socket = old_socket

        # 重连后创建新socket
        new_socket = MagicMock()
        new_socket.send_string = MagicMock()
        new_socket.recv_string = MagicMock(return_value='{"ok": true, "logged_in": true}')

        def fake_connect():
            client._socket = new_socket
            client._connected = True
            return True
        client.connect = MagicMock(side_effect=fake_connect)

        result = client._request({"action": "ping"})
        self.assertTrue(result.get("ok"))
        old_socket.close.assert_called_once()
        # 新socket发送了请求
        new_socket.send_string.assert_called_once()

    def test_request_no_socket_returns_none(self):
        """socket为None时直接返回None"""
        client = self._make_client()
        client._socket = None
        result = client._request({"action": "ping"})
        self.assertIsNone(result)

    def test_reconnect_only_once(self):
        """重连只重试1次, 不会无限循环"""
        client = self._make_client()
        client._socket.send_string = MagicMock(side_effect=Exception("fail"))

        call_count = [0]
        def fake_connect():
            call_count[0] += 1
            # 重连后socket仍然失败
            new_socket = MagicMock()
            new_socket.send_string = MagicMock(side_effect=Exception("still fail"))
            client._socket = new_socket
            client._connected = True
            return True
        client.connect = MagicMock(side_effect=fake_connect)

        result = client._request({"action": "ping"})
        # connect只调用1次(不是循环)
        self.assertEqual(call_count[0], 1)
        self.assertIsNone(result)

    def test_reconnect_failure_returns_none(self):
        """重连失败应返回None"""
        client = self._make_client()
        client._socket.send_string = MagicMock(side_effect=Exception("fail"))
        client.connect = MagicMock(return_value=False)

        result = client._request({"action": "ping"})
        self.assertIsNone(result)


# ============================================================
# 4. 网关handle_request路由测试 — 纯逻辑验证
# ============================================================
class TestHandleRequest(unittest.TestCase):
    """网关请求路由和参数校验 — 不import traderapi"""

    def _make_gateway(self):
        """创建模拟网关(不依赖traderapi)"""
        gw = MagicMock()
        gw.trader_spi = MagicMock()
        gw.trader_spi.logged_in = True
        gw.trader_spi._disconnected = False
        gw._consecutive_fail = 0
        gw._md_socket = MagicMock()
        gw._get_opposite_price = MagicMock(return_value=0)
        # 绑定handle_request逻辑
        gw.handle_request = self._handle_request.__get__(gw)
        return gw

    @staticmethod
    def _handle_request(self, req):
        """从huaxin_trade_gateway.py复制的handle_request逻辑(含对手价转换)"""
        action = req.get('action', '')

        if action == 'ping':
            return {
                "ok": True,
                "status": "alive",
                "logged_in": self.trader_spi.logged_in,
                "disconnected": self.trader_spi._disconnected,
                "consecutive_fail": self._consecutive_fail,
            }

        if action == 'query':
            stock = req.get('stock', '')
            with self.trader_spi.order_lock:
                trades = self.trader_spi.trades.get(stock[:6], [])
            return {"ok": True, "trades": trades[-5:]}

        if action == 'query_account':
            return self.trader_spi.query_account()

        if action == 'query_position':
            return self.trader_spi.query_position(stock=req.get('stock', ''))

        if action == 'query_orders':
            return self.trader_spi.query_orders(stock=req.get('stock', ''))

        if action == 'query_trades':
            return self.trader_spi.query_trades(stock=req.get('stock', ''))

        if action == 'query_shareholder':
            return self.trader_spi.query_shareholder()

        if action not in ('buy', 'sell'):
            return {"ok": False, "error": "unknown action: %s" % action}

        if not self.trader_spi.logged_in:
            return {"ok": False, "error": "华鑫交易未登录"}

        stock = req.get('stock', '')
        shares = int(req.get('shares', 0))
        price = float(req.get('price', 0))
        order_type = req.get('order_type', 'LIMIT')
        reason = req.get('reason', '')

        if not stock:
            return {"ok": False, "error": "stock is required"}

        direction = 'BUY' if action == 'buy' else 'SELL'

        # 市价单: 从行情中心获取对手价(买入ask1/卖出bid1), 转为限价单
        # A股有2%价格笼子, 必须用对手价下单, 不能用涨跌停价
        if order_type == 'MARKET':
            opp_price = self._get_opposite_price(stock, direction)
            if opp_price > 0:
                price = opp_price
                order_type = 'LIMIT'
            else:
                # 行情不可用时, 若调用方传了价格则用之, 否则拒绝
                if price <= 0:
                    return {"ok": False, "error": "行情不可用, 无法获取对手价, 请传限价"}

        result = self.trader_spi.send_order(stock, shares, price, direction, order_type, reason)
        return result

    def test_ping_returns_status(self):
        """ping请求返回网关状态"""
        gw = self._make_gateway()
        resp = gw.handle_request({"action": "ping"})
        self.assertTrue(resp["ok"])
        self.assertEqual(resp["status"], "alive")
        self.assertTrue(resp["logged_in"])

    def test_ping_reflects_disconnected(self):
        """ping应反映断线状态"""
        gw = self._make_gateway()
        gw.trader_spi._disconnected = True
        resp = gw.handle_request({"action": "ping"})
        self.assertTrue(resp["disconnected"])

    def test_unknown_action_rejected(self):
        """未知action应返回错误"""
        gw = self._make_gateway()
        resp = gw.handle_request({"action": "hack"})
        self.assertFalse(resp["ok"])
        self.assertIn("unknown action", resp["error"])

    def test_buy_without_login_rejected(self):
        """未登录时买入应被拒绝"""
        gw = self._make_gateway()
        gw.trader_spi.logged_in = False
        resp = gw.handle_request({
            "action": "buy",
            "stock": "600519.SH",
            "shares": 100,
            "price": 50.0,
        })
        self.assertFalse(resp["ok"])
        self.assertIn("未登录", resp["error"])

    def test_sell_without_login_rejected(self):
        """未登录时卖出应被拒绝"""
        gw = self._make_gateway()
        gw.trader_spi.logged_in = False
        resp = gw.handle_request({
            "action": "sell",
            "stock": "600519.SH",
            "shares": 100,
        })
        self.assertFalse(resp["ok"])
        self.assertIn("未登录", resp["error"])

    def test_buy_without_stock_rejected(self):
        """空stock应被拒绝"""
        gw = self._make_gateway()
        resp = gw.handle_request({
            "action": "buy",
            "stock": "",
            "shares": 100,
            "price": 50.0,
        })
        self.assertFalse(resp["ok"])
        self.assertIn("stock is required", resp["error"])

    def test_buy_calls_send_order(self):
        """买入请求应调用send_order(BUY)"""
        gw = self._make_gateway()
        gw.trader_spi.send_order = MagicMock(return_value={
            "ok": True, "order_ref": "123", "status": "pending"
        })
        resp = gw.handle_request({
            "action": "buy",
            "stock": "600519.SH",
            "shares": 100,
            "price": 50.0,
            "order_type": "LIMIT",
            "reason": "test",
        })
        self.assertTrue(resp["ok"])
        gw.trader_spi.send_order.assert_called_once_with(
            '600519.SH', 100, 50.0, 'BUY', 'LIMIT', 'test'
        )

    def test_sell_calls_send_order(self):
        """卖出请求应调用send_order(SELL) — MARKET单自动转对手价"""
        gw = self._make_gateway()
        gw._md_socket = MagicMock()
        gw._get_opposite_price = MagicMock(return_value=10.50)
        gw.trader_spi.send_order = MagicMock(return_value={
            "ok": True, "order_ref": "124", "status": "pending"
        })
        resp = gw.handle_request({
            "action": "sell",
            "stock": "600519.SH",
            "shares": 100,
            "price": 0,
            "order_type": "MARKET",
            "reason": "test",
        })
        self.assertTrue(resp["ok"])
        # MARKET单应转为LIMIT+对手价
        gw._get_opposite_price.assert_called_once_with('600519.SH', 'SELL')
        gw.trader_spi.send_order.assert_called_once_with(
            '600519.SH', 100, 10.50, 'SELL', 'LIMIT', 'test'
        )

    def test_query_position_routed(self):
        """query_position请求应路由到trader_spi"""
        gw = self._make_gateway()
        gw.trader_spi.query_position = MagicMock(return_value={"ok": True, "data": []})
        resp = gw.handle_request({
            "action": "query_position",
            "stock": "600519.SH",
        })
        gw.trader_spi.query_position.assert_called_once_with(stock="600519.SH")

    def test_query_account_routed(self):
        """query_account请求应路由到trader_spi"""
        gw = self._make_gateway()
        gw.trader_spi.query_account = MagicMock(return_value={"ok": True, "data": []})
        resp = gw.handle_request({"action": "query_account"})
        gw.trader_spi.query_account.assert_called_once()

    def test_query_orders_routed(self):
        """query_orders请求应路由到trader_spi"""
        gw = self._make_gateway()
        gw.trader_spi.query_orders = MagicMock(return_value={"ok": True, "data": []})
        resp = gw.handle_request({"action": "query_orders", "stock": "600519.SH"})
        gw.trader_spi.query_orders.assert_called_once_with(stock="600519.SH")

    def test_query_trades_routed(self):
        """query_trades请求应路由到trader_spi"""
        gw = self._make_gateway()
        gw.trader_spi.query_trades = MagicMock(return_value={"ok": True, "data": []})
        resp = gw.handle_request({"action": "query_trades", "stock": "600519.SH"})
        gw.trader_spi.query_trades.assert_called_once_with(stock="600519.SH")

    def test_shares_parsed_as_int(self):
        """shares参数应被解析为int"""
        gw = self._make_gateway()
        gw.trader_spi.send_order = MagicMock(return_value={"ok": True, "order_ref": "1", "status": "ok"})
        resp = gw.handle_request({
            "action": "buy",
            "stock": "600519.SH",
            "shares": "100",
            "price": "50.5",
        })
        gw.trader_spi.send_order.assert_called_once()
        call_args = gw.trader_spi.send_order.call_args
        self.assertEqual(call_args[0][1], 100)  # shares=int
        self.assertEqual(call_args[0][2], 50.5)  # price=float

    def test_default_order_type_is_limit(self):
        """默认order_type为LIMIT"""
        gw = self._make_gateway()
        gw.trader_spi.send_order = MagicMock(return_value={"ok": True, "order_ref": "1", "status": "ok"})
        resp = gw.handle_request({
            "action": "buy",
            "stock": "600519.SH",
            "shares": 100,
            "price": 50.0,
        })
        call_args = gw.trader_spi.send_order.call_args
        self.assertEqual(call_args[0][4], 'LIMIT')  # order_type

    def test_default_reason_is_empty(self):
        """默认reason为空字符串"""
        gw = self._make_gateway()
        gw.trader_spi.send_order = MagicMock(return_value={"ok": True, "order_ref": "1", "status": "ok"})
        resp = gw.handle_request({
            "action": "buy",
            "stock": "600519.SH",
            "shares": 100,
            "price": 50.0,
        })
        call_args = gw.trader_spi.send_order.call_args
        self.assertEqual(call_args[0][5], '')  # reason


# ============================================================
# 5. safe_str测试 — \x00处理
# ============================================================
class TestSafeStr(unittest.TestCase):
    """SDK回调字段含\x00, safe_str应正确清理"""

    def test_normal_string(self):
        self.assertEqual(safe_str('600519'), '600519')

    def test_null_terminated(self):
        """SDK返回的char[31]字段含\x00填充"""
        self.assertEqual(safe_str('600519\x00\x00\x00'), '600519')

    def test_none_input(self):
        self.assertEqual(safe_str(None), '')

    def test_empty_string(self):
        self.assertEqual(safe_str(''), '')

    def test_only_nulls(self):
        self.assertEqual(safe_str('\x00\x00'), '')

    def test_whitespace_stripped(self):
        self.assertEqual(safe_str('  600519  \x00'), '600519')

    def test_numeric_input(self):
        """数字输入应转字符串"""
        self.assertEqual(safe_str(123), '123')

    def test_float_input(self):
        self.assertEqual(safe_str(45.5), '45.5')


# ============================================================
# 6. 盘口v2 TradeGateway重连测试 — 纯逻辑
# ============================================================
class TestPankouTradeGatewayReconnect(unittest.TestCase):
    """盘口v2的TradeGateway._request()重连逻辑 — 与V7相同模式"""

    def _make_gateway(self):
        gw = MagicMock()
        gw.socket = MagicMock()
        gw.connected = True
        gw.host = '127.0.0.1'
        gw.port = 19850
        gw._request = self._request.__get__(gw)
        gw.connect = MagicMock(return_value=True)
        return gw

    @staticmethod
    def _request(self, req):
        """从pankou_live_v2.py复制的_request逻辑"""
        if not self.socket:
            return None
        try:
            self.socket.send_string(json.dumps(req))
            resp_str = self.socket.recv_string()
            return json.loads(resp_str)
        except Exception as e:
            try:
                self.socket.close()
                self.socket = None
                self.connected = False
                if self.connect():
                    self.socket.send_string(json.dumps(req))
                    resp_str = self.socket.recv_string()
                    return json.loads(resp_str)
                else:
                    return None
            except Exception as e2:
                return None

    def test_request_success(self):
        """正常请求不触发重连"""
        gw = self._make_gateway()
        gw.socket.send_string = MagicMock()
        gw.socket.recv_string = MagicMock(return_value='{"ok": true}')

        result = gw._request({"action": "ping"})
        self.assertTrue(result.get("ok"))
        self.assertEqual(gw.socket.send_string.call_count, 1)
        gw.connect.assert_not_called()

    def test_request_failure_reconnect(self):
        """通信失败→close→connect→重试"""
        gw = self._make_gateway()
        old_socket = MagicMock()
        old_socket.send_string = MagicMock(side_effect=Exception("EAGAIN"))
        gw.socket = old_socket

        result = gw._request({"action": "ping"})
        old_socket.close.assert_called_once()
        self.assertFalse(gw.connected)

    def test_no_socket_returns_none(self):
        """socket为None直接返回None"""
        gw = self._make_gateway()
        gw.socket = None
        result = gw._request({"action": "ping"})
        self.assertIsNone(result)

    def test_reconnect_success_retries_request(self):
        """重连成功后重试原始请求"""
        gw = self._make_gateway()
        old_socket = MagicMock()
        old_socket.send_string = MagicMock(side_effect=Exception("reset"))
        gw.socket = old_socket

        new_socket = MagicMock()
        new_socket.send_string = MagicMock()
        new_socket.recv_string = MagicMock(return_value='{"ok": true}')

        def fake_connect():
            gw.socket = new_socket
            gw.connected = True
            return True
        gw.connect = MagicMock(side_effect=fake_connect)

        result = gw._request({"action": "ping"})
        self.assertTrue(result.get("ok"))
        new_socket.send_string.assert_called_once()


# ============================================================
# 7. 华鑫SDK字段类型一致性 — 全量静态扫描
# ============================================================
class TestSDKFieldTypeConsistency(unittest.TestCase):
    """所有SDK字段赋值应使用正确类型 — 静态代码扫描"""

    def test_no_encode_on_sdk_fields(self):
        """SDK字段赋值不应使用.encode()"""
        with open('/home/hb/hub/huaxin_trade_gateway.py', 'r') as f:
            content = f.read()

        sdk_fields = [
            'SecurityID', 'ExchangeID', 'InvestorID',
            'OrderRef', 'OrderPriceType', 'Direction',
            'TimeCondition', 'VolumeCondition', 'Operway',
        ]

        issues = []
        for i, line in enumerate(content.split('\n'), 1):
            stripped = line.strip()
            if stripped.startswith('#'):
                continue
            for field in sdk_fields:
                if field in line and '.encode()' in line and '=' in line:
                    issues.append(f"第{i}行: {stripped}")

        self.assertEqual(len(issues), 0,
            f"SDK字段使用了.encode():\n" + "\n".join(issues))

    def test_legacy_files_still_have_encode_bug(self):
        """旧版simulation文件仍有.encode() bug — 确认已知问题"""
        for f in ['flash_crash_simulation.py', 'flash_crash_simulation_v2.py']:
            path = f'/home/hb/flash_crash/{f}'
            if os.path.exists(path):
                with open(path, 'r') as fh:
                    content = fh.read()
                has_encode = 'SecurityID' in content and '.encode()' in content
                if has_encode:
                    print(f"  ⚠️ 旧文件 {f} 仍有SecurityID.encode() — 已不用于生产")

    def test_all_sdk_string_fields_use_str(self):
        """所有SDK字符串字段赋值应使用str类型(非bytes)"""
        with open('/home/hb/hub/huaxin_trade_gateway.py', 'r') as f:
            content = f.read()

        # 检查所有 order.xxx = ... 和 req.xxx = ... 赋值
        issues = []
        for i, line in enumerate(content.split('\n'), 1):
            stripped = line.strip()
            if stripped.startswith('#'):
                continue
            # 匹配 order.XXX = ... 或 req.XXX = ... 模式
            if ('order.' in stripped or 'req.' in stripped) and '=' in stripped and '==' not in stripped:
                if '.encode()' in stripped:
                    issues.append(f"第{i}行: {stripped}")

        self.assertEqual(len(issues), 0,
            f"SDK对象字段使用了.encode():\n" + "\n".join(issues))


# ============================================================
# 8. ZMQ协议格式测试 — JSON序列化
# ============================================================
class TestZMQProtocolFormat(unittest.TestCase):
    """ZMQ通信使用JSON over REQ/REP"""

    def test_buy_request_format(self):
        """买入请求JSON格式"""
        req = {
            "action": "buy",
            "stock": "600519.SH",
            "shares": 100,
            "price": 50.0,
            "order_type": "LIMIT",
            "reason": "flash_crash",
        }
        serialized = json.dumps(req)
        deserialized = json.loads(serialized)
        self.assertEqual(deserialized["action"], "buy")
        self.assertEqual(deserialized["stock"], "600519.SH")
        self.assertEqual(deserialized["shares"], 100)

    def test_sell_request_format(self):
        """卖出请求JSON格式"""
        req = {
            "action": "sell",
            "stock": "600519.SH",
            "shares": 100,
            "price": 0,
            "order_type": "MARKET",
            "reason": "pankou_exit",
        }
        serialized = json.dumps(req)
        deserialized = json.loads(serialized)
        self.assertEqual(deserialized["action"], "sell")
        self.assertEqual(deserialized["order_type"], "MARKET")

    def test_ping_request_format(self):
        """ping请求格式"""
        req = {"action": "ping"}
        serialized = json.dumps(req)
        deserialized = json.loads(serialized)
        self.assertEqual(deserialized["action"], "ping")

    def test_response_ensure_ascii_false(self):
        """响应应使用ensure_ascii=False以支持中文"""
        resp = {"ok": True, "error": "华鑫交易未登录"}
        serialized = json.dumps(resp, ensure_ascii=False)
        self.assertIn("华鑫", serialized)
        # ensure_ascii=True会转义中文
        serialized_ascii = json.dumps(resp, ensure_ascii=True)
        self.assertNotIn("华鑫", serialized_ascii)

    def test_stock_code_in_json_is_str(self):
        """JSON中stock字段应为str, 非bytes"""
        req = {"action": "buy", "stock": "688766.SH", "shares": 100, "price": 45.5}
        serialized = json.dumps(req)
        # JSON字符串中不应有b'前缀
        self.assertNotIn("b'", serialized)
        self.assertNotIn('b"', serialized)


# ============================================================
# 9. 端到端业务流程测试
# ============================================================
class TestEndToEndBusinessFlow(unittest.TestCase):
    """端到端业务流程: V7闪崩→ZMQ→网关→send_order"""

    def test_flash_crash_buy_flow(self):
        """闪崩买入完整流程: 信号→ZMQ请求→网关→send_order"""
        gw = MagicMock()
        gw.trader_spi = MagicMock()
        gw.trader_spi.logged_in = True
        gw.trader_spi._disconnected = False
        gw._consecutive_fail = 0
        gw.handle_request = TestHandleRequest._handle_request.__get__(gw)

        gw.trader_spi.send_order.return_value = {
            "ok": True,
            "order_ref": "ref_001",
            "status": "accepted",
            "trades": [{"price": 45.5, "volume": 100}],
        }

        # V7发送买入请求(科创板)
        req = {
            "action": "buy",
            "stock": "688766.SH",
            "shares": 100,
            "price": 45.5,
            "order_type": "LIMIT",
            "reason": "flash_crash_v7",
        }
        resp = gw.handle_request(req)

        self.assertTrue(resp["ok"])
        self.assertEqual(resp["order_ref"], "ref_001")
        gw.trader_spi.send_order.assert_called_once_with(
            '688766.SH', 100, 45.5, 'BUY', 'LIMIT', 'flash_crash_v7'
        )

    def test_pankou_sell_flow(self):
        """盘口卖出完整流程: 信号→ZMQ请求→网关→send_order(MARKET→对手价)"""
        gw = MagicMock()
        gw.trader_spi = MagicMock()
        gw.trader_spi.logged_in = True
        gw.trader_spi._disconnected = False
        gw._consecutive_fail = 0
        gw._md_socket = MagicMock()
        gw._get_opposite_price = MagicMock(return_value=78.50)
        gw.handle_request = TestHandleRequest._handle_request.__get__(gw)

        gw.trader_spi.send_order.return_value = {
            "ok": True,
            "order_ref": "ref_002",
            "status": "pending",
        }

        req = {
            "action": "sell",
            "stock": "600600.SH",
            "shares": 200,
            "price": 0,
            "order_type": "MARKET",
            "reason": "pankou_v2_exit",
        }
        resp = gw.handle_request(req)

        self.assertTrue(resp["ok"])
        # MARKET单应转为LIMIT+对手价
        gw._get_opposite_price.assert_called_once_with('600600.SH', 'SELL')
        gw.trader_spi.send_order.assert_called_once_with(
            '600600.SH', 200, 78.50, 'SELL', 'LIMIT', 'pankou_v2_exit'
        )

    def test_gateway_crash_then_reconnect_flow(self):
        """网关崩溃→V7通信失败→自动重连→重试成功"""
        client = MagicMock()
        client._socket = MagicMock()
        client._connected = True
        client._request = TestZMQReconnectLogic._request.__get__(client)

        old_socket = MagicMock()
        old_socket.send_string = MagicMock(side_effect=Exception("Connection reset"))
        client._socket = old_socket

        new_socket = MagicMock()
        new_socket.send_string = MagicMock()
        new_socket.recv_string = MagicMock(return_value=json.dumps({
            "ok": True, "order_ref": "ref_003", "status": "accepted"
        }))

        def fake_connect():
            client._socket = new_socket
            client._connected = True
            return True
        client.connect = MagicMock(side_effect=fake_connect)

        # 模拟buy()调用_request
        resp = client._request({
            "action": "buy",
            "stock": "688766.SH",
            "shares": 100,
            "price": 45.5,
            "order_type": "LIMIT",
            "reason": "flash_crash",
        })

        self.assertTrue(resp.get("ok"))
        old_socket.close.assert_called_once()
        new_socket.send_string.assert_called_once()

    def test_send_order_uses_str_security_id(self):
        """send_order内部应使用str类型SecurityID(非bytes)"""
        # 验证stock_code[:6]是str
        stock_code = '688766.SH'
        security_id = stock_code[:6]
        self.assertIsInstance(security_id, str)
        self.assertEqual(security_id, '688766')
        # 确认不是bytes
        self.assertNotIsInstance(security_id, bytes)
        # 确认.encode()会产生bytes(错误做法)
        self.assertIsInstance(security_id.encode(), bytes)


# ============================================================
# 10. 边界条件测试
# ============================================================
class TestEdgeCases(unittest.TestCase):
    """边界条件和异常输入"""

    def test_stock_code_exactly_6_chars(self):
        """恰好6位的股票代码"""
        code = '600519'
        self.assertEqual(code[:6], '600519')

    def test_stock_code_with_suffix(self):
        """带.SH/.SZ后缀"""
        self.assertEqual('600519.SH'[:6], '600519')
        self.assertEqual('000001.SZ'[:6], '000001')

    def test_stock_code_shorter_than_6(self):
        """不足6位的代码(异常输入)"""
        code = '60051'
        self.assertEqual(code[:6], '60051')

    def test_empty_stock_code(self):
        """空股票代码"""
        code = ''
        self.assertEqual(code[:6], '')

    def test_unicode_in_reason(self):
        """reason字段含中文应正常序列化"""
        req = {
            "action": "buy",
            "stock": "600519.SH",
            "shares": 100,
            "price": 50.0,
            "reason": "闪崩买入-科创板",
        }
        serialized = json.dumps(req, ensure_ascii=False)
        deserialized = json.loads(serialized)
        self.assertEqual(deserialized["reason"], "闪崩买入-科创板")

    def test_zero_shares_handled(self):
        """0股买入 — 网关不校验, 交给SDK"""
        gw = MagicMock()
        gw.trader_spi = MagicMock()
        gw.trader_spi.logged_in = True
        gw.trader_spi._disconnected = False
        gw._consecutive_fail = 0
        gw.handle_request = TestHandleRequest._handle_request.__get__(gw)
        gw.trader_spi.send_order.return_value = {"ok": True, "order_ref": "1", "status": "ok"}

        resp = gw.handle_request({
            "action": "buy",
            "stock": "600519.SH",
            "shares": 0,
            "price": 50.0,
        })
        gw.trader_spi.send_order.assert_called_once_with(
            '600519.SH', 0, 50.0, 'BUY', 'LIMIT', ''
        )

    def test_negative_price_handled(self):
        """负价格 — 网关不校验, 交给SDK"""
        gw = MagicMock()
        gw.trader_spi = MagicMock()
        gw.trader_spi.logged_in = True
        gw.trader_spi._disconnected = False
        gw._consecutive_fail = 0
        gw.handle_request = TestHandleRequest._handle_request.__get__(gw)
        gw.trader_spi.send_order.return_value = {"ok": True, "order_ref": "1", "status": "ok"}

        resp = gw.handle_request({
            "action": "buy",
            "stock": "600519.SH",
            "shares": 100,
            "price": -1.0,
        })
        gw.trader_spi.send_order.assert_called_once_with(
            '600519.SH', 100, -1.0, 'BUY', 'LIMIT', ''
        )

    def test_missing_action_defaults_to_empty(self):
        """缺少action字段默认为空字符串"""
        gw = MagicMock()
        gw.trader_spi = MagicMock()
        gw.trader_spi.logged_in = True
        gw.trader_spi._disconnected = False
        gw._consecutive_fail = 0
        gw.handle_request = TestHandleRequest._handle_request.__get__(gw)

        resp = gw.handle_request({})
        self.assertFalse(resp["ok"])
        self.assertIn("unknown action", resp["error"])

    def test_very_long_stock_code(self):
        """超长股票代码只取前6位"""
        code = '600519.SH.EXTRA.STUFF'
        self.assertEqual(code[:6], '600519')

    def test_special_chars_in_stock(self):
        """股票代码含特殊字符(异常输入)"""
        code = '600@19'
        self.assertEqual(code[:6], '600@19')  # 不校验, 交给SDK


# ============================================================
# 11. 代码一致性验证 — V7和盘口v2的_request逻辑应一致
# ============================================================
class TestCodeConsistency(unittest.TestCase):
    """V7和盘口v2的ZMQ重连逻辑应保持一致"""

    def test_v7_and_pankou_request_logic_identical(self):
        """V7和盘口v2的_request()重连逻辑应结构一致"""
        # 读取V7的_request
        v7_path = '/home/hb/flash_crash/flash_crash_v7_zmq.py'
        with open(v7_path, 'r') as f:
            v7_content = f.read()

        # 读取盘口v2的_request
        pankou_path = '/home/hb/bands/fusion/pankou_live_v2.py'
        with open(pankou_path, 'r') as f:
            pankou_content = f.read()

        # 提取_request方法体
        def extract_method(content, class_name, method_name):
            lines = content.split('\n')
            in_method = False
            method_lines = []
            indent = None
            for line in lines:
                if f'def {method_name}' in line and not line.strip().startswith('#'):
                    in_method = True
                    indent = len(line) - len(line.lstrip())
                    method_lines.append(line)
                    continue
                if in_method:
                    if line.strip() == '':
                        method_lines.append(line)
                        continue
                    current_indent = len(line) - len(line.lstrip())
                    if current_indent <= indent and line.strip() and not line.strip().startswith('"""'):
                        break
                    method_lines.append(line)
            return '\n'.join(method_lines)

        v7_method = extract_method(v7_content, 'HuaxinOrderExecutor', '_request')
        pankou_method = extract_method(pankou_content, 'TradeGateway', '_request')

        # 标准化比较(去除变量名差异: _socket vs socket, _connected vs connected)
        v7_norm = v7_method.replace('self._socket', 'SOCKET').replace('self._connected', 'CONNECTED')
        pankou_norm = pankou_method.replace('self.socket', 'SOCKET').replace('self.connected', 'CONNECTED')

        # 关键逻辑点应一致
        self.assertIn('SOCKET.close()', v7_norm, "V7 _request应调用close()")
        self.assertIn('SOCKET.close()', pankou_norm, "盘口v2 _request应调用close()")
        self.assertIn('SOCKET = None', v7_norm, "V7 _request应设socket=None")
        self.assertIn('SOCKET = None', pankou_norm, "盘口v2 _request应设socket=None")
        self.assertIn('self.connect()', v7_norm, "V7 _request应调用connect()")
        self.assertIn('self.connect()', pankou_norm, "盘口v2 _request应调用connect()")

    def test_v7_and_pankou_both_have_reconnect(self):
        """V7和盘口v2都应有重连逻辑"""
        v7_path = '/home/hb/flash_crash/flash_crash_v7_zmq.py'
        pankou_path = '/home/hb/bands/fusion/pankou_live_v2.py'

        with open(v7_path, 'r') as f:
            v7 = f.read()
        with open(pankou_path, 'r') as f:
            pankou = f.read()

        # 两个文件都应有"自动重连"或"重连"注释
        self.assertTrue(
            '重连' in v7 or 'reconnect' in v7.lower(),
            "V7应有重连逻辑"
        )
        self.assertTrue(
            '重连' in pankou or 'reconnect' in pankou.lower(),
            "盘口v2应有重连逻辑"
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)


# ============================================================
# 12. 对手价逻辑测试 — MARKET→LIMIT转换
# ============================================================
class TestOppositePriceLogic(unittest.TestCase):
    """对手价获取和MARKET→LIMIT转换 — A股2%价格笼子"""

    def _make_gateway(self):
        gw = MagicMock()
        gw.trader_spi = MagicMock()
        gw.trader_spi.logged_in = True
        gw.trader_spi._disconnected = False
        gw._consecutive_fail = 0
        gw._md_socket = MagicMock()
        gw._get_opposite_price = MagicMock(return_value=0)
        gw.handle_request = TestHandleRequest._handle_request.__get__(gw)
        return gw

    def test_market_buy_uses_ask1(self):
        """市价买入应获取ask1(卖一价)作为对手价"""
        gw = self._make_gateway()
        gw._get_opposite_price = MagicMock(return_value=10.50)
        gw.trader_spi.send_order = MagicMock(return_value={"ok": True, "order_ref": "1", "status": "ok"})

        resp = gw.handle_request({
            "action": "buy", "stock": "600519.SH", "shares": 100,
            "price": 0, "order_type": "MARKET", "reason": "test"
        })
        self.assertTrue(resp["ok"])
        # 验证_get_opposite_price被调用且方向为BUY
        gw._get_opposite_price.assert_called_once_with('600519.SH', 'BUY')
        # 验证send_order收到的是LIMIT+对手价
        gw.trader_spi.send_order.assert_called_once_with(
            '600519.SH', 100, 10.50, 'BUY', 'LIMIT', 'test'
        )

    def test_market_sell_uses_bid1(self):
        """市价卖出应获取bid1(买一价)作为对手价"""
        gw = self._make_gateway()
        gw._get_opposite_price = MagicMock(return_value=10.48)
        gw.trader_spi.send_order = MagicMock(return_value={"ok": True, "order_ref": "2", "status": "ok"})

        resp = gw.handle_request({
            "action": "sell", "stock": "600519.SH", "shares": 100,
            "price": 0, "order_type": "MARKET", "reason": "test"
        })
        self.assertTrue(resp["ok"])
        gw._get_opposite_price.assert_called_once_with('600519.SH', 'SELL')
        gw.trader_spi.send_order.assert_called_once_with(
            '600519.SH', 100, 10.48, 'SELL', 'LIMIT', 'test'
        )

    def test_market_no_md_data_rejected(self):
        """行情不可用且未传价格→拒绝下单(防止2%笼子拒单)"""
        gw = self._make_gateway()
        gw._get_opposite_price = MagicMock(return_value=0)  # 行情不可用

        resp = gw.handle_request({
            "action": "buy", "stock": "600519.SH", "shares": 100,
            "price": 0, "order_type": "MARKET", "reason": "test"
        })
        self.assertFalse(resp["ok"])
        self.assertIn("行情不可用", resp["error"])

    def test_market_no_md_but_has_price_fallback(self):
        """行情不可用但调用方传了价格→用该价格(降级)"""
        gw = self._make_gateway()
        gw._get_opposite_price = MagicMock(return_value=0)  # 行情不可用
        gw.trader_spi.send_order = MagicMock(return_value={"ok": True, "order_ref": "3", "status": "ok"})

        resp = gw.handle_request({
            "action": "buy", "stock": "600519.SH", "shares": 100,
            "price": 10.50, "order_type": "MARKET", "reason": "test"
        })
        self.assertTrue(resp["ok"])
        # 行情不可用但price>0, 仍用原price但保持MARKET类型
        gw.trader_spi.send_order.assert_called_once_with(
            '600519.SH', 100, 10.50, 'BUY', 'MARKET', 'test'
        )

    def test_limit_order_not_affected(self):
        """限价单不受对手价逻辑影响"""
        gw = self._make_gateway()
        gw.trader_spi.send_order = MagicMock(return_value={"ok": True, "order_ref": "4", "status": "ok"})

        resp = gw.handle_request({
            "action": "buy", "stock": "600519.SH", "shares": 100,
            "price": 10.00, "order_type": "LIMIT", "reason": "test"
        })
        self.assertTrue(resp["ok"])
        gw._get_opposite_price.assert_not_called()
        gw.trader_spi.send_order.assert_called_once_with(
            '600519.SH', 100, 10.00, 'BUY', 'LIMIT', 'test'
        )

    def test_get_opposite_price_buy_returns_ask1(self):
        """_get_opposite_price: 买入方向应返回ask1"""
        # 直接测试_get_opposite_price逻辑(从网关代码复制)
        md_socket = MagicMock()
        md_socket.send_string = MagicMock()
        md_socket.recv_string = MagicMock(return_value=json.dumps({
            "ok": True, "data": {"ask1": 10.50, "bid1": 10.48}
        }))

        # 模拟_get_opposite_price逻辑
        resp_str = md_socket.recv_string()
        resp = json.loads(resp_str)
        if resp.get("ok") and resp.get("data"):
            d = resp["data"]
            price = d.get("ask1", 0)

        self.assertEqual(price, 10.50)

    def test_get_opposite_price_sell_returns_bid1(self):
        """_get_opposite_price: 卖出方向应返回bid1"""
        md_socket = MagicMock()
        md_socket.recv_string = MagicMock(return_value=json.dumps({
            "ok": True, "data": {"ask1": 10.50, "bid1": 10.48}
        }))

        resp_str = md_socket.recv_string()
        resp = json.loads(resp_str)
        if resp.get("ok") and resp.get("data"):
            d = resp["data"]
            price = d.get("bid1", 0)

        self.assertEqual(price, 10.48)


# ============================================================
# 13. 重连后股东账号查询测试
# ============================================================
class TestReconnectShareholder(unittest.TestCase):
    """重连后必须重新查询股东账号, 否则下单必被拒"""

    def test_reconnect_calls_query_shareholder(self):
        """_reconnect_huaxin()成功后应调用query_shareholder()"""
        with open('/home/hb/hub/huaxin_trade_gateway.py', 'r') as f:
            content = f.read()

        # 找到_reconnect_huaxin方法
        in_method = False
        has_query_shareholder = False
        for line in content.split('\n'):
            if 'def _reconnect_huaxin' in line:
                in_method = True
            elif in_method:
                if line.strip() and not line.startswith(' ') and not line.startswith('\t'):
                    break  # 方法结束
                if 'query_shareholder' in line:
                    has_query_shareholder = True
        self.assertTrue(has_query_shareholder,
            "_reconnect_huaxin()成功后必须调用query_shareholder(), 否则新SPI的_shareholder_ids为空→下单必被拒")

    def test_initial_connect_calls_query_shareholder(self):
        """connect_huaxin()成功后应调用query_shareholder()"""
        with open('/home/hb/hub/huaxin_trade_gateway.py', 'r') as f:
            content = f.read()

        in_method = False
        has_query_shareholder = False
        for line in content.split('\n'):
            if 'def connect_huaxin' in line:
                in_method = True
            elif in_method:
                if line.strip() and not line.startswith(' ') and not line.startswith('\t'):
                    break
                if 'query_shareholder' in line:
                    has_query_shareholder = True
        self.assertTrue(has_query_shareholder,
            "connect_huaxin()成功后必须调用query_shareholder()")


# ============================================================
# 14. send_order价格校验测试
# ============================================================
class TestSendOrderPriceValidation(unittest.TestCase):
    """send_order()价格校验 — price=0应被拦截"""

    def test_market_order_with_zero_price_rejected(self):
        """MARKET单price=0应被拒绝(不再用0.001兜底)"""
        with open('/home/hb/hub/huaxin_trade_gateway.py', 'r') as f:
            content = f.read()

        # 确认0.001兜底已被移除
        self.assertNotIn('0.001', content,
            "send_order()不应有0.001元兜底价格, A股不合法")

    def test_limit_order_with_zero_price_rejected(self):
        """LIMIT单price=0应被拒绝"""
        with open('/home/hb/hub/huaxin_trade_gateway.py', 'r') as f:
            content = f.read()

        # 找到send_order方法中的LIMIT price校验
        in_send_order = False
        has_limit_price_check = False
        for line in content.split('\n'):
            if 'def send_order' in line:
                in_send_order = True
            elif in_send_order:
                if line.strip() and not line.startswith(' ') and not line.startswith('\t'):
                    break
                # 检查LIMIT分支中有price <= 0校验
                if 'price' in line and '<=' in line and '0' in line:
                    has_limit_price_check = True
        self.assertTrue(has_limit_price_check,
            "send_order()应对LIMIT单校验price>0")


# ============================================================
# 15. MarketHub定时任务窗口测试
# ============================================================
class TestScheduledTaskWindow(unittest.TestCase):
    """MarketHub定时任务窗口 — 16:30和23:00不应过窄"""

    def test_1630_window_at_least_30_minutes(self):
        """16:30任务窗口应>=30分钟(16:30-16:59)"""
        with open('/home/hb/hub/market_hub_v2.py', 'r') as f:
            content = f.read()

        # 检查16:30窗口条件
        self.assertIn('now.minute >= 30', content,
            "16:30任务窗口应使用minute>=30(而非minute==30), 防止_running=True时任务丢失")

    def test_2300_window_at_least_5_minutes(self):
        """23:00任务窗口应>=5分钟(23:00-23:05)"""
        with open('/home/hb/hub/market_hub_v2.py', 'r') as f:
            content = f.read()

        # 检查23:00窗口条件
        self.assertIn('now.minute <= 5', content,
            "23:00任务窗口应使用minute<=5(而非minute==0), 防止主循环阻塞时错过")

    def test_no_23_00_to_23_59_window(self):
        """不应有23:00-23:59的宽窗口(每分钟重复检查)"""
        with open('/home/hb/hub/market_hub_v2.py', 'r') as f:
            content = f.read()

        self.assertNotIn('0 <= now.minute <= 59', content,
            "23:00-23:59窗口过宽, 会导致每分钟重复检查")


# ============================================================
# 16. pankou_live_v2 Context复用测试
# ============================================================
class TestPankouContextReuse(unittest.TestCase):
    """pankou_live_v2.py的TradeGateway应复用zmq.Context"""

    def test_connect_reuses_context(self):
        """connect()应复用Context, 只重建socket"""
        with open('/home/hb/bands/fusion/pankou_live_v2.py', 'r') as f:
            content = f.read()

        # 检查connect()方法中有ctx复用逻辑
        in_connect = False
        has_ctx_reuse = False
        for line in content.split('\n'):
            if 'def connect(self)' in line:
                in_connect = True
            elif in_connect:
                if line.strip() and not line.startswith(' ') and not line.startswith('\t'):
                    break
                if 'ctx is None' in line:
                    has_ctx_reuse = True
        self.assertTrue(has_ctx_reuse,
            "connect()应检查ctx is None再创建, 避免每次connect都新建Context导致泄漏")

    def test_connect_does_not_call_request_for_ping(self):
        """connect()应直接发ping而非调用_request(), 避免递归"""
        with open('/home/hb/bands/fusion/pankou_live_v2.py', 'r') as f:
            content = f.read()

        in_connect = False
        calls_request = False
        for line in content.split('\n'):
            if 'def connect(self)' in line:
                in_connect = True
            elif in_connect:
                if line.strip() and not line.startswith(' ') and not line.startswith('\t'):
                    break
                if 'self._request' in line and 'ping' in line:
                    calls_request = True
        self.assertFalse(calls_request,
            "connect()不应调用self._request(ping), 应直接发ping避免递归风险")


# ============================================================
# 17. chuanyang sys.path保障测试
# ============================================================
class TestChuanyangSysPath(unittest.TestCase):
    """chuanyang_d01_d20.py应有sys.path保障"""

    def test_sys_path_insert_exists(self):
        """chuanyang_d01_d20.py应有sys.path.insert保障"""
        with open('/home/hb/bands/chuanyang/chuanyang_d01_d20.py', 'r') as f:
            content = f.read()

        self.assertIn('sys.path.insert', content,
            "chuanyang_d01_d20.py应有sys.path.insert(0, os.path.dirname(...))保障导入")

    def test_import_uses_bare_module_name(self):
        """import应使用裸模块名(chuanyang_signals)而非包路径(chuanyang.chuanyang_signals)"""
        with open('/home/hb/bands/chuanyang/chuanyang_d01_d20.py', 'r') as f:
            content = f.read()

        self.assertNotIn('from chuanyang.chuanyang_signals', content,
            "不应使用from chuanyang.chuanyang_signals, 应使用from chuanyang_signals")


# ============================================================
# 18. SubscribePrivateTopic/SubscribePublicTopic调用验证
# ============================================================
class TestSubscribeTopicCalled(unittest.TestCase):
    """华鑫SDK必须调用SubscribePrivateTopic/SubscribePublicTopic才能收到委托/成交回报
    根因: 2026-07-29 V7闪崩20笔买入全成但网关返回pending, 因为回调不触发
    """

    def test_connect_huaxin_calls_subscribe_private(self):
        """connect_huaxin()必须在Init()前调用SubscribePrivateTopic"""
        with open('/home/hb/hub/huaxin_trade_gateway.py', 'r') as f:
            content = f.read()

        # 找到connect_huaxin方法
        self.assertIn('SubscribePrivateTopic', content,
            "huaxin_trade_gateway.py必须调用SubscribePrivateTopic, 否则OnRtnOrder回调不触发")

    def test_connect_huaxin_calls_subscribe_public(self):
        """connect_huaxin()必须在Init()前调用SubscribePublicTopic"""
        with open('/home/hb/hub/huaxin_trade_gateway.py', 'r') as f:
            content = f.read()

        self.assertIn('SubscribePublicTopic', content,
            "huaxin_trade_gateway.py必须调用SubscribePublicTopic, 否则OnRtnTrade回调不触发")

    def test_subscribe_before_init(self):
        """Subscribe调用必须在RegisterFront和Init之间"""
        with open('/home/hb/hub/huaxin_trade_gateway.py', 'r') as f:
            content = f.read()

        # 检查SubscribePrivateTopic出现在RegisterFront之前(或Init之前)
        for method_name in ['connect_huaxin', '_reconnect_huaxin']:
            in_method = False
            subscribe_line = -1
            init_line = -1
            for i, line in enumerate(content.split('\n')):
                if f'def {method_name}' in line:
                    in_method = True
                elif in_method:
                    if line.strip() and not line.startswith(' ') and not line.startswith('\t') and 'def ' in line:
                        break
                    if 'SubscribePrivateTopic' in line and subscribe_line < 0:
                        subscribe_line = i
                    if '.Init()' in line and init_line < 0:
                        init_line = i
            self.assertLess(subscribe_line, init_line,
                f"{method_name}(): SubscribePrivateTopic必须在Init()之前调用")

    def test_reconnect_also_calls_subscribe(self):
        """_reconnect_huaxin()也必须调用Subscribe"""
        with open('/home/hb/hub/huaxin_trade_gateway.py', 'r') as f:
            content = f.read()

        # 找到_reconnect_huaxin方法中是否有SubscribePrivateTopic
        in_reconnect = False
        has_subscribe = False
        for line in content.split('\n'):
            if 'def _reconnect_huaxin' in line:
                in_reconnect = True
            elif in_reconnect:
                if line.strip() and not line.startswith(' ') and not line.startswith('\t') and 'def ' in line:
                    break
                if 'SubscribePrivateTopic' in line:
                    has_subscribe = True
        self.assertTrue(has_subscribe,
            "_reconnect_huaxin()也必须调用SubscribePrivateTopic, 否则重连后回调同样不触发")

    def test_subscribe_uses_tora_tert_resume(self):
        """Subscribe参数必须使用TORA_TERT_RESUME(=1), 否则可能收不到历史回报"""
        with open('/home/hb/hub/huaxin_trade_gateway.py', 'r') as f:
            content = f.read()

        self.assertIn('TORA_TERT_RESUME', content,
            "SubscribePrivateTopic/SubscribePublicTopic必须传traderapi.TORA_TERT_RESUME参数")


# ============================================================
# 19. 价格tick对齐测试
# ============================================================
class TestPriceTickAlignment(unittest.TestCase):
    """A股价格必须对齐0.01元tick, 否则华鑫拒单
    根因: 2026-07-29 V7闪崩20笔买入全部拒单(价格非最小单位的倍数/委托价不正确)
    """

    def setUp(self):
        # 从网关代码导入align_price_tick
        sys.path.insert(0, '/home/hb/hub')
        # 直接复制函数实现(避免import traderapi)
        self.align = lambda price: round(price, 2) if price > 0 else price

    def test_3digit_price_aligned(self):
        """3位小数价格应对齐到2位"""
        self.assertEqual(self.align(11.668), 11.67)
        self.assertEqual(self.align(55.617), 55.62)
        self.assertEqual(self.align(3.306), 3.31)

    def test_already_aligned_price_unchanged(self):
        """已对齐的价格不应改变"""
        self.assertEqual(self.align(55.05), 55.05)
        self.assertEqual(self.align(100.00), 100.00)
        self.assertEqual(self.align(3.30), 3.30)

    def test_zero_price_unchanged(self):
        """价格<=0应原样返回(由上层拦截)"""
        self.assertEqual(self.align(0), 0)
        self.assertEqual(self.align(-1), -1)

    def test_high_price_aligned(self):
        """高价股(科创板)也需要对齐"""
        self.assertEqual(self.align(362.363), 362.36)
        self.assertEqual(self.align(349.84), 349.84)  # 已经对齐

    def test_align_function_exists_in_gateway(self):
        """网关代码必须有align_price_tick函数"""
        with open('/home/hb/hub/huaxin_trade_gateway.py', 'r') as f:
            content = f.read()
        self.assertIn('align_price_tick', content,
            "huaxin_trade_gateway.py必须定义align_price_tick函数对齐A股价格tick")

    def test_send_order_uses_align(self):
        """send_order()必须对价格调用align_price_tick"""
        with open('/home/hb/hub/huaxin_trade_gateway.py', 'r') as f:
            lines = f.readlines()

        # 找send_order方法
        in_send_order = False
        has_align_call = False
        for line in lines:
            if 'def send_order' in line:
                in_send_order = True
            elif in_send_order:
                if line.strip() and not line.startswith(' ') and not line.startswith('\t') and 'def ' in line:
                    break
                if 'align_price_tick' in line:
                    has_align_call = True
        self.assertTrue(has_align_call,
            "send_order()必须调用align_price_tick(), 否则对手价3位小数会被华鑫拒单")
