#!/usr/bin/env python3.7
"""
华鑫交易网关 — ZMQ REP服务
============================
独立进程(Python 3.7 + 华鑫SDK)，接收策略进程的下单/查询指令。

架构:
  策略进程(Python3.13) ──ZMQ REQ──▸ 华鑫交易网关(Python3.7) ──▸ 华鑫仿真交易前置
                                     tcp://127.0.0.1:19850

协议(JSON over ZMQ):
  交易: {"action": "buy"|"sell", "stock": "600519.SH", "shares": 100, ...}
  查询: {"action": "query_account"|"query_position"|"query_orders"|"query_trades", ...}
  心跳: {"action": "ping"}
  响应: {"ok": true|false, ...}

华鑫仿真账户: 00032127 / 88358920
交易前置: tcp://210.14.72.21:4400

运行: conda activate huaxin && python -u huaxin_trade_gateway.py [--port 19850]
"""
import sys, os, json, time, argparse, threading
from datetime import datetime
from collections import defaultdict

# ============================================================
# 日志
# ============================================================
LOG_FILE = '/home/hb/flash_crash/trade_gateway.log'

def log(msg):
    ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    line = "[%s] %s" % (ts, msg)
    print(line, flush=True)
    try:
        with open(LOG_FILE, 'a') as f:
            f.write(line + '\n')
    except:
        pass

# ============================================================
# 华鑫SDK
# ============================================================
SDK_LIB = '/home/hb/flash_crash/huaxin_sdk/lib'
sys.path.insert(0, SDK_LIB)

import zmq
import traderapi

# ============================================================
# 配置
# ============================================================
CONFIG = {
    'td_front': 'tcp://210.14.72.21:4400',
    'account_id': '00032127',
    'password': '88358920',
    'zmq_port': 19850,
}

def stock_to_exchange(code):
    """股票代码→华鑫交易所ID"""
    c = code[:6]
    if c.startswith('6'):
        return traderapi.TORA_TSTP_EXD_SSE
    else:
        return traderapi.TORA_TSTP_EXD_SZSE

def exchange_to_str(exchange_id):
    """华鑫交易所ID→字符串"""
    if exchange_id == traderapi.TORA_TSTP_EXD_SSE:
        return 'SSE'
    elif exchange_id == traderapi.TORA_TSTP_EXD_SZSE:
        return 'SZSE'
    return str(exchange_id)

def safe_str(val):
    """安全转字符串，处理\\x00"""
    if val is None:
        return ''
    s = str(val)
    return s.strip('\x00').strip()

# ============================================================
# 华鑫交易SPI
# ============================================================
class HuaxinTraderSpi(traderapi.CTORATstpTraderSpi):
    def __init__(self, api, cfg):
        traderapi.CTORATstpTraderSpi.__init__(self)
        self.__api = api
        self.cfg = cfg
        self.logged_in = False
        self.login_event = threading.Event()
        self.order_ref = 0
        self.order_lock = threading.Lock()
        self.order_status = {}   # {order_ref: {stock, status, ...}}
        self.trades = defaultdict(list)  # {stock: [{price, volume, direction}]}
        self.trade_event = threading.Event()
        self.pending_orders = {}  # {order_ref: result_event}
        # 查询相关
        self._qry_id = 0
        self._qry_results = {}   # {request_id: {'event': Event, 'data': []}}
        self._qry_lock = threading.Lock()
        # 重连相关
        self._disconnected = False
        self._reconnect_requested = False

    def _next_qry_id(self):
        with self._qry_lock:
            self._qry_id += 1
            return self._qry_id

    # ======== 连接/登录 ========

    def OnFrontConnected(self):
        log("✅ 华鑫交易前置连接成功")
        self._disconnected = False
        self._login()

    def OnFrontDisconnected(self, nReason):
        log("❌ 华鑫交易断开, reason=%s" % nReason)
        self.logged_in = False
        self._disconnected = True
        self._reconnect_requested = True

    def _login(self):
        req = traderapi.CTORATstpReqUserLoginField()
        req.LogInAccount = self.cfg['account_id']
        req.Password = self.cfg['password']
        req.LogInAccountType = traderapi.TORA_TSTP_LACT_AccountID
        self.__api.ReqUserLogin(req, 1)

    def OnRspUserLogin(self, pRspUserLoginField, pRspInfoField, nRequestID):
        try:
            if pRspInfoField is None:
                return
            if pRspInfoField.ErrorID == 0:
                log("✅ 华鑫交易登录成功 (账户: %s)" % self.cfg['account_id'])
                self.logged_in = True
                self.login_event.set()
            else:
                log("❌ 交易登录失败: %s %s" % (pRspInfoField.ErrorID, pRspInfoField.ErrorMsg))
                self.login_event.set()
        except Exception as e:
            log("⚠️ OnRspUserLogin异常: %s" % e)
            self.login_event.set()

    # ======== 下单回报 ========

    def OnRspOrderInsert(self, pInputOrderField, pRspInfoField, nRequestID):
        try:
            if pRspInfoField and pRspInfoField.ErrorID != 0:
                log("❌ 下单被拒: %s %s" % (pRspInfoField.ErrorID, pRspInfoField.ErrorMsg))
                if pInputOrderField:
                    ref = pInputOrderField.OrderRef
                    with self.order_lock:
                        if ref in self.pending_orders:
                            self.pending_orders[ref]['error'] = pRspInfoField.ErrorMsg
                            self.pending_orders[ref]['event'].set()
        except Exception as e:
            log("⚠️ OnRspOrderInsert异常: %s" % e)

    def OnRtnOrder(self, pOrderField):
        try:
            if pOrderField is None:
                return
            ref = pOrderField.OrderRef
            status = pOrderField.OrderStatus
            stock = pOrderField.SecurityID.strip('\x00')
            sm = {
                traderapi.TORA_TSTP_OST_Accepted: '已报',
                traderapi.TORA_TSTP_OST_PartTraded: '部成',
                traderapi.TORA_TSTP_OST_AllTraded: '全成',
                traderapi.TORA_TSTP_OST_AllCanceled: '全撤',
                traderapi.TORA_TSTP_OST_Rejected: '拒单',
            }
            ss = sm.get(status, str(status))
            with self.order_lock:
                self.order_status[ref] = {
                    'stock': stock, 'status': ss,
                    'volume_original': pOrderField.VolumeTotalOriginal,
                    'volume_traded': pOrderField.VolumeTraded,
                }
            if status in (traderapi.TORA_TSTP_OST_AllTraded,
                         traderapi.TORA_TSTP_OST_AllCanceled,
                         traderapi.TORA_TSTP_OST_Rejected):
                log("📋 订单回报: %s %s 委托%s成%s" % (stock, ss, pOrderField.VolumeTotalOriginal, pOrderField.VolumeTraded))
                with self.order_lock:
                    if ref in self.pending_orders:
                        self.pending_orders[ref]['status'] = ss
                        self.pending_orders[ref]['event'].set()
        except Exception as e:
            log("订单回报异常: %s" % e)

    def OnRtnTrade(self, pTradeField):
        try:
            if pTradeField is None:
                return
            stock = pTradeField.SecurityID.strip('\x00')
            price = pTradeField.Price
            vol = pTradeField.Volume
            d = '买入' if pTradeField.Direction == traderapi.TORA_TSTP_D_Buy else '卖出'
            with self.order_lock:
                self.trades[stock].append({'price': price, 'volume': vol, 'direction': d})
            log("💰 成交回报: %s %s %s股 @ %.3f" % (stock, d, vol, price))
            self.trade_event.set()
        except Exception as e:
            log("成交回报异常: %s" % e)

    def OnErrRtnOrderInsert(self, pInputOrderField, pRspInfoField, nRequestID):
        try:
            if pRspInfoField:
                log("❌ 下单错误: %s %s" % (pRspInfoField.ErrorID, pRspInfoField.ErrorMsg))
                if pInputOrderField:
                    ref = pInputOrderField.OrderRef
                    with self.order_lock:
                        if ref in self.pending_orders:
                            self.pending_orders[ref]['error'] = pRspInfoField.ErrorMsg
                            self.pending_orders[ref]['event'].set()
        except Exception as e:
            log("下单错误回报异常: %s" % e)

    # ======== 查询回报 ========

    def OnRspQryTradingAccount(self, pTradingAccountField, pRspInfoField, nRequestID, bIsLast):
        """资金查询回报"""
        try:
            with self._qry_lock:
                if nRequestID not in self._qry_results:
                    return
            if pTradingAccountField:
                item = {
                    'account_id': safe_str(pTradingAccountField.AccountID),
                    'pre_deposit': pTradingAccountField.PreDeposit,
                    'useful_money': pTradingAccountField.UsefulMoney,
                    'frozen_cash': pTradingAccountField.FrozenCash,
                    'frozen_commission': pTradingAccountField.FrozenCommission,
                    'commission': pTradingAccountField.Commission,
                    'deposit': pTradingAccountField.Deposit,
                    'withdraw': pTradingAccountField.Withdraw,
                }
                with self._qry_lock:
                    self._qry_results[nRequestID]['data'].append(item)
            if bIsLast:
                with self._qry_lock:
                    if nRequestID in self._qry_results:
                        self._qry_results[nRequestID]['event'].set()
        except Exception as e:
            log("⚠️ 资金查询回报异常: %s" % e)

    def OnRspQryPosition(self, pPositionField, pRspInfoField, nRequestID, bIsLast):
        """持仓查询回报"""
        try:
            with self._qry_lock:
                if nRequestID not in self._qry_results:
                    return
            if pPositionField:
                item = {
                    'exchange': exchange_to_str(pPositionField.ExchangeID),
                    'security_id': safe_str(pPositionField.SecurityID),
                    'security_name': safe_str(pPositionField.SecurityName),
                    'current_position': pPositionField.CurrentPosition,
                    'available_position': pPositionField.AvailablePosition,
                    'history_pos': pPositionField.HistoryPos,
                    'today_bs_pos': pPositionField.TodayBSPos,
                    'history_pos_price': pPositionField.HistoryPosPrice,
                    'total_pos_cost': pPositionField.TotalPosCost,
                    'close_profit': pPositionField.CloseProfit,
                    'today_commission': pPositionField.TodayCommission,
                    'today_total_buy_amount': pPositionField.TodayTotalBuyAmount,
                    'today_total_sell_amount': pPositionField.TodayTotalSellAmount,
                }
                with self._qry_lock:
                    self._qry_results[nRequestID]['data'].append(item)
            if bIsLast:
                with self._qry_lock:
                    if nRequestID in self._qry_results:
                        self._qry_results[nRequestID]['event'].set()
        except Exception as e:
            log("⚠️ 持仓查询回报异常: %s" % e)

    def OnRspQryOrder(self, pOrderField, pRspInfoField, nRequestID, bIsLast):
        """委托查询回报"""
        try:
            with self._qry_lock:
                if nRequestID not in self._qry_results:
                    return
            if pOrderField:
                dm = {traderapi.TORA_TSTP_D_Buy: '买入', traderapi.TORA_TSTP_D_Sell: '卖出'}
                sm = {
                    traderapi.TORA_TSTP_OST_Accepted: '已报',
                    traderapi.TORA_TSTP_OST_PartTraded: '部成',
                    traderapi.TORA_TSTP_OST_AllTraded: '全成',
                    traderapi.TORA_TSTP_OST_AllCanceled: '全撤',
                    traderapi.TORA_TSTP_OST_Rejected: '拒单',
                }
                item = {
                    'exchange': exchange_to_str(pOrderField.ExchangeID),
                    'security_id': safe_str(pOrderField.SecurityID),
                    'direction': dm.get(pOrderField.Direction, str(pOrderField.Direction)),
                    'order_status': sm.get(pOrderField.OrderStatus, str(pOrderField.OrderStatus)),
                    'limit_price': pOrderField.LimitPrice,
                    'volume_original': pOrderField.VolumeTotalOriginal,
                    'volume_traded': pOrderField.VolumeTraded,
                    'volume_canceled': pOrderField.VolumeCanceled,
                    'order_ref': pOrderField.OrderRef,
                    'order_sys_id': safe_str(pOrderField.OrderSysID),
                    'insert_time': safe_str(pOrderField.InsertTime),
                    'trading_day': safe_str(pOrderField.TradingDay),
                    'status_msg': safe_str(pOrderField.StatusMsg),
                }
                with self._qry_lock:
                    self._qry_results[nRequestID]['data'].append(item)
            if bIsLast:
                with self._qry_lock:
                    if nRequestID in self._qry_results:
                        self._qry_results[nRequestID]['event'].set()
        except Exception as e:
            log("⚠️ 委托查询回报异常: %s" % e)

    def OnRspQryTrade(self, pTradeField, pRspInfoField, nRequestID, bIsLast):
        """成交查询回报"""
        try:
            with self._qry_lock:
                if nRequestID not in self._qry_results:
                    return
            if pTradeField:
                dm = {traderapi.TORA_TSTP_D_Buy: '买入', traderapi.TORA_TSTP_D_Sell: '卖出'}
                item = {
                    'exchange': exchange_to_str(pTradeField.ExchangeID),
                    'security_id': safe_str(pTradeField.SecurityID),
                    'trade_id': safe_str(pTradeField.TradeID),
                    'direction': dm.get(pTradeField.Direction, str(pTradeField.Direction)),
                    'price': pTradeField.Price,
                    'volume': pTradeField.Volume,
                    'trade_date': safe_str(pTradeField.TradeDate),
                    'trade_time': safe_str(pTradeField.TradeTime),
                    'trading_day': safe_str(pTradeField.TradingDay),
                    'order_ref': pTradeField.OrderRef,
                }
                with self._qry_lock:
                    self._qry_results[nRequestID]['data'].append(item)
            if bIsLast:
                with self._qry_lock:
                    if nRequestID in self._qry_results:
                        self._qry_results[nRequestID]['event'].set()
        except Exception as e:
            log("⚠️ 成交查询回报异常: %s" % e)

    # ======== 交易接口 ========

    def send_order(self, stock_code, shares, price, direction, order_type='LIMIT', reason=''):
        """发送委托单"""
        shares = (shares // 100) * 100
        if shares <= 0:
            return {"ok": False, "error": "shares must be >= 100"}

        with self.order_lock:
            self.order_ref += 1
            ref = self.order_ref
            result_event = threading.Event()
            self.pending_orders[ref] = {
                'event': result_event,
                'status': None,
                'error': None,
            }

        order = traderapi.CTORATstpInputOrderField()
        order.InvestorID = self.cfg['account_id']
        order.SecurityID = stock_code[:6].encode()
        order.ExchangeID = stock_to_exchange(stock_code)
        order.Direction = traderapi.TORA_TSTP_D_Buy if direction == 'BUY' else traderapi.TORA_TSTP_D_Sell
        if order_type == 'MARKET':
            order.OrderPriceType = traderapi.TORA_TSTP_OPT_AnyPrice
            order.LimitPrice = 0
        else:
            order.OrderPriceType = traderapi.TORA_TSTP_OPT_LimitPrice
            order.LimitPrice = price
        order.VolumeTotalOriginal = shares
        order.TimeCondition = traderapi.TORA_TSTP_TC_GFD
        order.VolumeCondition = traderapi.TORA_TSTP_VC_AV
        order.OrderRef = ref
        order.Operway = traderapi.TORA_TSTP_OPERW_PCClient

        ret = self.__api.ReqOrderInsert(order, ref)
        ds = '买入' if direction == 'BUY' else '卖出'
        pstr = "%.3f" % price if price else '市价'
        log("📤 委托%s: %s %s股 @ %s ref=%s ret=%s 原因=%s" % (ds, stock_code, shares, pstr, ref, ret, reason))

        if ret != 0:
            with self.order_lock:
                del self.pending_orders[ref]
            return {"ok": False, "error": "ReqOrderInsert返回%s" % ret}

        result_event.wait(timeout=5.0)

        with self.order_lock:
            pending = self.pending_orders.pop(ref, {})
            status = pending.get('status')
            error = pending.get('error')

        if error:
            return {"ok": False, "error": error}

        if status:
            stock_trades = []
            with self.order_lock:
                for t in self.trades.get(stock_code[:6], []):
                    stock_trades.append(t)
            return {
                "ok": True,
                "order_ref": ref,
                "status": status,
                "trades": stock_trades[-3:],
            }

        return {
            "ok": True,
            "order_ref": ref,
            "status": "pending",
            "error": "委托已发出，5秒内未收到最终回报",
        }

    # ======== 查询接口 ========

    def query_account(self):
        """查询资金账户"""
        req_id = self._next_qry_id()
        event = threading.Event()
        with self._qry_lock:
            self._qry_results[req_id] = {'event': event, 'data': []}

        req = traderapi.CTORATstpQryTradingAccountField()
        req.InvestorID = self.cfg['account_id']
        ret = self.__api.ReqQryTradingAccount(req, req_id)

        if ret != 0:
            with self._qry_lock:
                del self._qry_results[req_id]
            return {"ok": False, "error": "ReqQryTradingAccount返回%s" % ret}

        event.wait(timeout=5.0)
        with self._qry_lock:
            result = self._qry_results.pop(req_id, {'data': []})

        log("📊 资金查询: %d条记录" % len(result['data']))
        return {"ok": True, "data": result['data']}

    def query_position(self, stock=''):
        """查询持仓"""
        req_id = self._next_qry_id()
        event = threading.Event()
        with self._qry_lock:
            self._qry_results[req_id] = {'event': event, 'data': []}

        req = traderapi.CTORATstpQryPositionField()
        req.InvestorID = self.cfg['account_id']
        if stock:
            req.SecurityID = stock[:6]
            req.ExchangeID = stock_to_exchange(stock)
        ret = self.__api.ReqQryPosition(req, req_id)

        if ret != 0:
            with self._qry_lock:
                del self._qry_results[req_id]
            return {"ok": False, "error": "ReqQryPosition返回%s" % ret}

        event.wait(timeout=5.0)
        with self._qry_lock:
            result = self._qry_results.pop(req_id, {'data': []})

        log("📊 持仓查询: %d条记录" % len(result['data']))
        return {"ok": True, "data": result['data']}

    def query_orders(self, stock=''):
        """查询委托"""
        req_id = self._next_qry_id()
        event = threading.Event()
        with self._qry_lock:
            self._qry_results[req_id] = {'event': event, 'data': []}

        req = traderapi.CTORATstpQryOrderField()
        req.InvestorID = self.cfg['account_id']
        if stock:
            req.SecurityID = stock[:6]
            req.ExchangeID = stock_to_exchange(stock)
        ret = self.__api.ReqQryOrder(req, req_id)

        if ret != 0:
            with self._qry_lock:
                del self._qry_results[req_id]
            return {"ok": False, "error": "ReqQryOrder返回%s" % ret}

        event.wait(timeout=5.0)
        with self._qry_lock:
            result = self._qry_results.pop(req_id, {'data': []})

        log("📊 委托查询: %d条记录" % len(result['data']))
        return {"ok": True, "data": result['data']}

    def query_trades(self, stock=''):
        """查询成交"""
        req_id = self._next_qry_id()
        event = threading.Event()
        with self._qry_lock:
            self._qry_results[req_id] = {'event': event, 'data': []}

        req = traderapi.CTORATstpQryTradeField()
        req.InvestorID = self.cfg['account_id']
        if stock:
            req.SecurityID = stock[:6]
            req.ExchangeID = stock_to_exchange(stock)
        ret = self.__api.ReqQryTrade(req, req_id)

        if ret != 0:
            with self._qry_lock:
                del self._qry_results[req_id]
            return {"ok": False, "error": "ReqQryTrade返回%s" % ret}

        event.wait(timeout=5.0)
        with self._qry_lock:
            result = self._qry_results.pop(req_id, {'data': []})

        log("📊 成交查询: %d条记录" % len(result['data']))
        return {"ok": True, "data": result['data']}


# ============================================================
# ZMQ交易网关服务
# ============================================================
class TradeGateway:
    def __init__(self, cfg):
        self.cfg = cfg
        self.trader_api = None
        self.trader_spi = None
        self.zmq_ctx = None
        self.zmq_socket = None
        # 健康检查
        self._health_thread = None
        self._running = False
        self._last_ping_ok = True
        self._consecutive_fail = 0

    def connect_huaxin(self):
        """连接华鑫交易前置"""
        log("连接华鑫交易前置: %s" % self.cfg['td_front'])
        self.trader_api = traderapi.CTORATstpTraderApi.CreateTstpTraderApi('')
        self.trader_spi = HuaxinTraderSpi(self.trader_api, self.cfg)
        self.trader_api.RegisterSpi(self.trader_spi)
        self.trader_api.RegisterFront(self.cfg['td_front'])
        self.trader_api.Init()

        if not self.trader_spi.login_event.wait(timeout=30):
            log("❌ 华鑫交易登录超时")
            return False
        if not self.trader_spi.logged_in:
            log("❌ 华鑫交易登录失败")
            return False
        return True

    def _reconnect_huaxin(self):
        """华鑫交易断线重连: 销毁旧API, 重建新连接"""
        try:
            if self.trader_api:
                try:
                    self.trader_api.Release()
                except Exception:
                    pass
        except Exception as e:
            log("⚠️ 释放旧API异常: %s" % e)

        log("🔄 华鑫交易重连中...")
        self.trader_api = traderapi.CTORATstpTraderApi.CreateTstpTraderApi('')
        self.trader_spi = HuaxinTraderSpi(self.trader_api, self.cfg)
        self.trader_api.RegisterSpi(self.trader_spi)
        self.trader_api.RegisterFront(self.cfg['td_front'])
        self.trader_api.Init()

        if not self.trader_spi.login_event.wait(timeout=30):
            log("❌ 华鑫交易重连登录超时")
            return False
        if not self.trader_spi.logged_in:
            log("❌ 华鑫交易重连登录失败")
            return False
        log("✅ 华鑫交易重连成功")
        self._consecutive_fail = 0
        return True

    def _is_trading_hours(self):
        """是否在交易时段 (9:10~15:10), 午休11:35~12:55不算断线"""
        now = datetime.now()
        t = now.hour * 100 + now.minute
        # 盘前: 9:10 ~ 9:30
        if 910 <= t < 930:
            return True
        # 上午盘: 9:30 ~ 11:35
        if 930 <= t <= 1135:
            return True
        # 午休: 不检测
        if 1135 < t < 1255:
            return False
        # 下午盘: 12:55 ~ 15:10
        if 1255 <= t <= 1510:
            return True
        return False

    def _is_lunch_break(self):
        """是否午休时段"""
        t = datetime.now().hour * 100 + datetime.now().minute
        return 1130 <= t < 1300

    def _health_check_loop(self):
        """健康检查线程: 盘中每分钟检测, 断线自动重连"""
        log("🏥 健康检查线程启动 (盘中每分钟检测)")
        while self._running:
            try:
                time.sleep(60)
                if not self._running:
                    break
                # 非交易时段不检测
                if not self._is_trading_hours():
                    continue

                spi = self.trader_spi
                if spi is None:
                    continue

                # 检查1: SDK回调已标记断线 → 立即重连
                if spi._disconnected or not spi.logged_in:
                    self._consecutive_fail += 1
                    log("⚠️ 华鑫交易状态异常 (disconnected=%s logged_in=%s 连续失败%d次)" %
                        (spi._disconnected, spi.logged_in, self._consecutive_fail))

                    if not self._is_lunch_break():
                        log("🔄 触发自动重连...")
                        if self._reconnect_huaxin():
                            log("✅ 自动重连成功")
                            self._last_ping_ok = True
                        else:
                            log("❌ 自动重连失败, 下轮继续")
                            self._last_ping_ok = False
                    else:
                        log("⏸️ 午休时段, 延后重连")
                    continue

                # 检查2: ping检测 — 主动发查询确认连接存活
                try:
                    result = spi.query_account()
                    if result.get('ok'):
                        self._consecutive_fail = 0
                        self._last_ping_ok = True
                    else:
                        self._consecutive_fail += 1
                        log("⚠️ 华鑫查询异常: %s (连续%d次)" % (result, self._consecutive_fail))
                        self._last_ping_ok = False
                except Exception as e:
                    self._consecutive_fail += 1
                    log("⚠️ 华鑫查询超时: %s (连续%d次)" % (e, self._consecutive_fail))
                    self._last_ping_ok = False

                # 连续3次失败触发重连
                if self._consecutive_fail >= 3 and not self._is_lunch_break():
                    log("⚠️ 连续%d次健康检查失败, 触发主动重连!" % self._consecutive_fail)
                    if self._reconnect_huaxin():
                        log("✅ 主动重连成功")
                    else:
                        log("❌ 主动重连失败, 下轮继续")

            except Exception as e:
                log("⚠️ 健康检查异常: %s" % e)

    def start_zmq(self, port):
        """启动ZMQ REP服务"""
        self.zmq_ctx = zmq.Context()
        self.zmq_socket = self.zmq_ctx.socket(zmq.REP)
        addr = "tcp://*:%d" % port
        self.zmq_socket.bind(addr)
        log("✅ ZMQ交易网关: %s" % addr)
        return True

    def handle_request(self, req):
        """处理请求"""
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
            # 旧接口兼容
            stock = req.get('stock', '')
            with self.trader_spi.order_lock:
                trades = self.trader_spi.trades.get(stock[:6], [])
            return {"ok": True, "trades": trades[-5:]}

        # === 新增查询接口 ===
        if action == 'query_account':
            return self.trader_spi.query_account()

        if action == 'query_position':
            return self.trader_spi.query_position(stock=req.get('stock', ''))

        if action == 'query_orders':
            return self.trader_spi.query_orders(stock=req.get('stock', ''))

        if action == 'query_trades':
            return self.trader_spi.query_trades(stock=req.get('stock', ''))

        # === 交易接口 ===
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
        result = self.trader_spi.send_order(stock, shares, price, direction, order_type, reason)
        return result

    def run(self, port):
        """主循环"""
        if not self.connect_huaxin():
            log("❌ 华鑫交易连接失败，退出")
            return False

        if not self.start_zmq(port):
            log("❌ ZMQ启动失败，退出")
            return False

        log("=" * 60)
        log("⚡ 华鑫交易网关已启动")
        log("  交易前置: %s" % self.cfg['td_front'])
        log("  账户: %s" % self.cfg['account_id'])
        log("  ZMQ: tcp://*:%d" % port)
        log("  接口: buy|sell|ping|query|query_account|query_position|query_orders|query_trades")
        log("  健康检查: 盘中每分钟 (断线自动重连+重登)")
        log("  等待策略进程请求...")
        log("=" * 60)

        # 启动健康检查线程
        self._running = True
        self._health_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        self._health_thread.start()

        try:
            while self._running:
                msg = self.zmq_socket.recv_string()
                try:
                    req = json.loads(msg)
                except:
                    self.zmq_socket.send_string(json.dumps({"ok": False, "error": "invalid JSON"}))
                    continue

                resp = self.handle_request(req)
                self.zmq_socket.send_string(json.dumps(resp, ensure_ascii=False))

                action = req.get('action', '?')
                stock = req.get('stock', '')
                if action not in ('ping',):
                    log("📋 ZMQ请求: %s %s → ok=%s" % (action, stock, resp.get('ok')))

        except KeyboardInterrupt:
            log("用户中断")
        except Exception as e:
            log("❌ 网关异常: %s" % e)
        finally:
            self._running = False
            self.cleanup()

    def cleanup(self):
        if self.zmq_socket:
            self.zmq_socket.close()
        if self.zmq_ctx:
            self.zmq_ctx.term()
        log("网关退出")

# ============================================================
# 入口
# ============================================================
def main():
    parser = argparse.ArgumentParser(description='华鑫交易网关 — ZMQ REP服务')
    parser.add_argument('--port', type=int, default=CONFIG['zmq_port'],
                        help='ZMQ REP端口 (默认: 19850)')
    args = parser.parse_args()

    gw = TradeGateway(CONFIG)
    gw.run(args.port)

if __name__ == '__main__':
    main()
