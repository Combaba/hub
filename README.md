# Hub — 统一行情中心 + 华鑫交易网关

A股交易系统核心基础设施，提供实时行情分发和仿真交易网关服务。

## 组件

| 组件 | 文件 | 端口 | 说明 |
|---|---|---|---|
| **MarketHub v2** | `market_hub_v2.py` | :19800-19803 | 统一行情中心 — ZMQ订阅herzqt实时行情 + 米筐日更 + FastAPI |
| **华鑫交易网关** | `huaxin_trade_gateway.py` | :19850 | 华鑫SDK仿真交易 — ZMQ REP买卖委托 + 查询接口 |

## MarketHub v2

从herzqt订阅全市场A股3秒快照，聚合K线，保存tick，提供多协议行情服务。

### 数据流
```
herzqt:19002 ──SUB──▸ MarketHub v2
                      ├─ ZMQ PUB :19800  [snap]  快照分发
                      ├─ ZMQ PUB :19801  [bar]   1m K线分发
                      ├─ ZMQ REP :19802  [query] ZMQ查询
                      ├─ FastAPI :19803  [http]  Web API
                      └─ 文件保存 tick→parquet
```

### 接口

**ZMQ REP :19802**
| 命令 | 请求 | 说明 |
|---|---|---|
| `snapshot` | `{"cmd":"snapshot","code":"600000.SH"}` | 单只股票最新快照(含5档盘口) |
| `bars` | `{"cmd":"bars","code":"600000.SH","count":120}` | 实时1分钟K线 |
| `tick` | `{"cmd":"tick","code":"600000.SH","date":"2026-07-23"}` | tick数据 |
| `stats` | `{"cmd":"stats"}` | 运行统计 |

**FastAPI :19803**
| 端点 | 说明 |
|---|---|
| `GET /api/snapshot/{code}` | 单只快照 |
| `GET /api/snapshots?index_only=false` | 全部快照 |
| `GET /api/tick/{code}` | tick查询 |
| `GET /api/bars/1m/{code}` | 实时1m K线 |
| `GET /api/bars/5m/{code}` | 实时5m K线 |
| `GET /api/history/1m/{code}` | 历史1m K线(parquet) |
| `GET /api/history/5m/{code}` | 历史5m K线(parquet) |
| `GET /api/history/daily/{code}` | 历史日线(parquet) |
| `GET /api/stats` | 运行统计 |
| `POST /api/rqdata/update?mode=today` | 手动触发米筐下载 |
| `POST /api/rqdata/update?mode=backfill` | 手动触发历史补齐 |

### 定时任务
| 时间 | 任务 |
|---|---|
| 16:30 | 米筐当日下载(全市场A股 1m/5m/daily) |
| 23:00 | 米筐历史补齐(4Key轮换，配额耗尽即停) |

### 运行
```bash
python3 market_hub_v2.py [--zmq-host HOST] [--zmq-port PORT] \
                         [--snap-port 19800] [--bar-port 19801] \
                         [--query-port 19802] [--api-port 19803]
```

## 华鑫交易网关

华鑫快速交易SDK的ZMQ封装，支持买卖委托和4个查询接口。Python 3.7 (华鑫SDK限制)。

### 接口 (ZMQ REP :19850)

| Action | 请求 | 说明 |
|---|---|---|
| `ping` | `{"action":"ping"}` | 心跳+登录状态 |
| `buy` | `{"action":"buy","stock":"600000.SH","shares":100,"price":10.5,"order_type":"LIMIT"}` | 买入委托 |
| `sell` | `{"action":"sell","stock":"600000.SH","shares":100,"price":0,"order_type":"MARKET"}` | 卖出委托 |
| `query_account` | `{"action":"query_account"}` | 查询账户资金 |
| `query_position` | `{"action":"query_position"}` | 查询持仓 |
| `query_orders` | `{"action":"query_orders"}` | 查询委托 |
| `query_trades` | `{"action":"query_trades"}` | 查询成交 |

### 运行
```bash
conda activate huaxin
python -u huaxin_trade_gateway.py
```

## 测试

```bash
# 单元测试(不需要服务运行)
pytest test_market_hub_v2.py -v
pytest test_trading_system_integration.py -v -m unit

# 集成测试(需要MarketHub + 华鑫网关运行)
pytest test_trading_system_integration.py -v -m "integration and not live"

# 盘中实战测试
pytest test_trading_system_integration.py -v -m live
```

## 数据存储
| 数据 | 路径 |
|---|---|
| 实时tick | `/data/hb_data/stock_tick/{code}/{YYYY-MM-DD}.parquet` |
| 开盘5分钟tick | `/data/hb_data/stock_open_tick/{code}/{YYYY-MM-DD}.parquet` |
| 1分钟K线 | `/data/hb_data/stock_data/minute/{code}.parquet` |
| 5分钟K线 | `/data/hb_data/stock_data/5min/{code}.parquet` |
| 日线 | `/data/hb_data/stock_data/daily/{code}.parquet` |
| 指数1m | `/data/hb_data/stock_data/index_1m/{code}_1m.parquet` |
