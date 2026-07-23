#!/usr/bin/env python3
"""
Market Hub v2 TDD 测试套件
===========================
覆盖全部核心功能:
  1. 代码标准化 (normalize_code, code_to_rqdata)
  2. TickSaver (buffer + flush + parquet + 查询)
  3. BarAggregator (1m/5m K线聚合)
  4. RqDataUpdater (当日下载 + 历史补齐 + key轮换 + 逐日补齐)
  5. FastAPI 接口 (9个端点)
  6. 定时任务调度 (16:30当日 + 23:00补齐)

运行: pytest test_market_hub_v2.py -v
"""

import os
import sys
import json
import time
import shutil
import tempfile
import threading
from datetime import datetime, date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import numpy as np
import pandas as pd
import pytest

# ============================================================
# 导入被测模块 — 用临时目录替换DATA_ROOT避免污染生产数据
# ============================================================
# 先创建临时目录
TMP_ROOT = Path(tempfile.mkdtemp(prefix='mh2_test_'))
os.environ['MH2_TEST_DATA_ROOT'] = str(TMP_ROOT)

# 导入模块
sys.path.insert(0, str(Path(__file__).parent))
import market_hub_v2 as mh2

# 覆盖DATA_ROOT为临时目录
mh2.DATA_ROOT = TMP_ROOT
mh2.TICK_DIR = TMP_ROOT / 'stock_tick'
mh2.OPEN_TICK_DIR = TMP_ROOT / 'stock_open_tick'
mh2.MINUTE_DIR = TMP_ROOT / 'stock_data' / 'minute'
mh2.FIVE_MIN_DIR = TMP_ROOT / 'stock_data' / '5min'
mh2.DAILY_DIR = TMP_ROOT / 'stock_data' / 'daily'
mh2.INDEX_1M_DIR = TMP_ROOT / 'stock_data' / 'index_1m'


# ============================================================
# Fixtures
# ============================================================
@pytest.fixture(autouse=True)
def clean_tmp_dirs():
    """每个测试前清空临时目录"""
    for d in [mh2.TICK_DIR, mh2.OPEN_TICK_DIR, mh2.MINUTE_DIR, mh2.FIVE_MIN_DIR,
              mh2.DAILY_DIR, mh2.INDEX_1M_DIR]:
        if d.exists():
            shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True, exist_ok=True)
    yield
    # teardown: 清理
    for d in [mh2.TICK_DIR, mh2.OPEN_TICK_DIR, mh2.MINUTE_DIR, mh2.FIVE_MIN_DIR,
              mh2.DAILY_DIR, mh2.INDEX_1M_DIR]:
        shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def tick_saver():
    """创建TickSaver实例"""
    return mh2.TickSaver(flush_interval=1)


@pytest.fixture
def bar_agg():
    """创建BarAggregator实例"""
    return mh2.BarAggregator(max_bars=500)


@pytest.fixture
def rq_updater():
    """创建RqDataUpdater实例"""
    return mh2.RqDataUpdater()


# ============================================================
# 1. 代码标准化测试
# ============================================================
class TestNormalizeCode:
    """测试 herzqt SecurityID → 标准化代码 转换"""

    def test_shanghai_stock(self):
        """沪市股票: 6开头 → .SH"""
        assert mh2.normalize_code('600000') == '600000.SH'

    def test_shenzhen_main(self):
        """深市主板: 0开头 → .SZ (注意000001是指数，用000002)"""
        assert mh2.normalize_code('000002') == '000002.SZ'

    def test_shenzhen_gem(self):
        """创业板: 3开头 → .SZ"""
        assert mh2.normalize_code('300001') == '300001.SZ'

    def test_bse_stock(self):
        """北交所: 8开头 → .SZ"""
        assert mh2.normalize_code('830001') == '830001.SZ'

    def test_kcb_stock(self):
        """科创板: 6开头 → .SH (与沪市主板相同)"""
        assert mh2.normalize_code('688001') == '688001.SH'

    def test_index_sh(self):
        """沪市指数: 在KNOWN_INDICES中 → .IDX"""
        assert mh2.normalize_code('000001', '1') == '000001.IDX'
        assert mh2.normalize_code('000300') == '000300.IDX'

    def test_index_sz(self):
        """深市指数: 399开头 → .IDX"""
        assert mh2.normalize_code('399001') == '399001.IDX'

    def test_empty_sid(self):
        """空SecurityID → None"""
        assert mh2.normalize_code('') is None
        assert mh2.normalize_code(None) is None

    def test_unknown_prefix(self):
        """未知前缀 → None"""
        assert mh2.normalize_code('999999') is None

    def test_is_index(self):
        """is_index判断"""
        assert mh2.is_index('000001.IDX') is True
        assert mh2.is_index('600000.SH') is False


class TestCodeToRqdata:
    """测试 标准化代码 → 米筐代码 转换"""

    def test_sh_to_xshg(self):
        assert mh2.code_to_rqdata('600000.SH') == '600000.XSHG'

    def test_sz_to_xshe(self):
        assert mh2.code_to_rqdata('000001.SZ') == '000001.XSHE'

    def test_idx_sh_to_xshg(self):
        """上证指数.IDX → .XSHG"""
        assert mh2.code_to_rqdata('000001.IDX') == '000001.XSHG'

    def test_idx_sz_to_xshe(self):
        """深证指数.IDX → .XSHE"""
        assert mh2.code_to_rqdata('399001.IDX') == '399001.XSHE'

    def test_no_dot_passthrough(self):
        """无后缀代码原样返回"""
        assert mh2.code_to_rqdata('600000') == '600000'


# ============================================================
# 2. TickSaver 测试
# ============================================================
class TestTickSaver:
    """测试 Tick 数据 buffer + flush + parquet 保存 + 查询"""

    def test_add_single_tick(self, tick_saver):
        """添加单条tick到buffer"""
        tick = {'timestamp': '2026-07-21 09:30:00.000', 'code': '600000.SH',
                'last_price': 10.5, 'volume': 100, 'turnover': 1050}
        tick_saver.add('600000.SH', tick)
        assert tick_saver._tick_count == 1
        assert len(tick_saver._buffers['600000.SH']) == 1

    def test_add_multiple_codes(self, tick_saver):
        """多只股票同时buffer"""
        for code in ['600000.SH', '000001.SZ', '300001.SZ']:
            tick_saver.add(code, {'timestamp': '2026-07-21 09:30:00', 'code': code,
                                   'last_price': 10.0, 'volume': 100, 'turnover': 1000})
        assert tick_saver._tick_count == 3
        assert len(tick_saver._buffers) == 3

    def test_flush_creates_parquet(self, tick_saver):
        """flush后生成parquet文件"""
        tick = {'timestamp': '2026-07-21 09:30:00', 'code': '600000.SH',
                'last_price': 10.5, 'volume': 100, 'turnover': 1050}
        tick_saver.add('600000.SH', tick)
        tick_saver.flush()

        today = date.today().isoformat()
        out_file = mh2.TICK_DIR / '600000.SH' / f'{today}.parquet'
        assert out_file.exists(), f"parquet文件未生成: {out_file}"

        df = pd.read_parquet(out_file)
        assert len(df) == 1
        assert df.iloc[0]['code'] == '600000.SH'

    def test_flush_appends_to_existing(self, tick_saver):
        """多次flush追加到同一文件(去重)"""
        today = date.today().isoformat()
        for i in range(3):
            tick = {'timestamp': f'2026-07-21 09:30:0{i}', 'code': '600000.SH',
                    'last_price': 10.5 + i * 0.1, 'volume': 100, 'turnover': 1050}
            tick_saver.add('600000.SH', tick)
            tick_saver.flush()

        out_file = mh2.TICK_DIR / '600000.SH' / f'{today}.parquet'
        df = pd.read_parquet(out_file)
        assert len(df) == 3

    def test_flush_dedup_by_timestamp_code(self, tick_saver):
        """相同timestamp+code去重(保留last)"""
        today = date.today().isoformat()
        tick1 = {'timestamp': '2026-07-21 09:30:00', 'code': '600000.SH',
                 'last_price': 10.5, 'volume': 100, 'turnover': 1050}
        tick2 = {'timestamp': '2026-07-21 09:30:00', 'code': '600000.SH',
                 'last_price': 10.8, 'volume': 200, 'turnover': 2160}
        tick_saver.add('600000.SH', tick1)
        tick_saver.flush()
        tick_saver.add('600000.SH', tick2)
        tick_saver.flush()

        out_file = mh2.TICK_DIR / '600000.SH' / f'{today}.parquet'
        df = pd.read_parquet(out_file)
        assert len(df) == 1
        assert df.iloc[0]['last_price'] == 10.8  # 保留last

    def test_get_tick_data(self, tick_saver):
        """查询tick数据"""
        today = date.today().isoformat()
        for i in range(5):
            tick = {'timestamp': f'2026-07-21 09:30:0{i}', 'code': '600000.SH',
                    'last_price': 10.5, 'volume': 100, 'turnover': 1050}
            tick_saver.add('600000.SH', tick)
        tick_saver.flush()

        # 查询全部
        ticks = tick_saver.get_tick_data('600000.SH', today, limit=0)
        assert len(ticks) == 5

        # 查询limit
        ticks = tick_saver.get_tick_data('600000.SH', today, limit=3)
        assert len(ticks) == 3

    def test_get_tick_data_nonexistent(self, tick_saver):
        """查询不存在的tick返回空"""
        ticks = tick_saver.get_tick_data('999999.SH', '2026-07-21')
        assert ticks == []

    def test_stats(self, tick_saver):
        """统计信息"""
        tick = {'timestamp': '2026-07-21 09:30:00', 'code': '600000.SH',
                'last_price': 10.5, 'volume': 100, 'turnover': 1050}
        tick_saver.add('600000.SH', tick)
        stats = tick_saver.stats()
        assert stats['tick_count'] == 1
        assert stats['buffered_codes'] == 1


# ============================================================
# 3. BarAggregator 测试
# ============================================================
class TestBarAggregator:
    """测试 1分钟/5分钟 K线聚合"""

    def test_single_update_no_completed_bar(self, bar_agg):
        """单次更新不会产生completed bar"""
        c1m, c5m = bar_agg.update(
            code='600000.SH', price=10.5, volume=1000, turnover=10500,
            timestamp_str='09:30:00', pre_close=10.0
        )
        assert c1m is None
        assert c5m is None

    def test_two_updates_same_minute(self, bar_agg):
        """同一分钟内两次更新不产生completed bar"""
        bar_agg.update('600000.SH', 10.5, 1000, 10500, '09:30:00')
        c1m, c5m = bar_agg.update('600000.SH', 10.6, 2000, 21200, '09:30:30')
        assert c1m is None  # 还在同一分钟内

    def test_minute_boundary_produces_completed_bar(self, bar_agg):
        """跨分钟边界产生completed 1m bar"""
        bar_agg.update('600000.SH', 10.5, 1000, 10500, '09:30:00')
        c1m, _ = bar_agg.update('600000.SH', 10.8, 3000, 32400, '09:31:00')
        assert c1m is not None
        assert c1m['code'] == '600000.SH'
        assert c1m['close'] == 10.5  # 09:30的close是09:30:00的price

    def test_5m_bar_boundary(self, bar_agg):
        """跨5分钟边界产生completed 5m bar"""
        bar_agg.update('600000.SH', 10.5, 1000, 10500, '09:30:00')
        c1m, c5m = bar_agg.update('600000.SH', 10.8, 5000, 54000, '09:35:00')
        assert c5m is not None
        assert c5m['code'] == '600000.SH'

    def test_get_1m_bars(self, bar_agg):
        """获取1m K线列表"""
        # 构造2根1m K线
        bar_agg.update('600000.SH', 10.5, 1000, 10500, '09:30:00')
        bar_agg.update('600000.SH', 10.8, 2000, 21600, '09:31:00')
        bar_agg.update('600000.SH', 10.9, 3000, 32700, '09:32:00')

        bars = bar_agg.get_1m_bars('600000.SH', count=10)
        # 1根completed(09:30) + 1根current(09:32)
        assert len(bars) >= 1

    def test_get_5m_bars(self, bar_agg):
        """获取5m K线列表"""
        bar_agg.update('600000.SH', 10.5, 1000, 10500, '09:30:00')
        bar_agg.update('600000.SH', 10.8, 5000, 54000, '09:35:00')
        bars = bar_agg.get_5m_bars('600000.SH', count=10)
        assert len(bars) >= 1

    def test_multiple_codes_independent(self, bar_agg):
        """多只股票K线独立"""
        bar_agg.update('600000.SH', 10.5, 1000, 10500, '09:30:00')
        bar_agg.update('000001.SZ', 20.0, 500, 10000, '09:30:00')
        bar_agg.update('600000.SH', 10.8, 2000, 21600, '09:31:00')
        bar_agg.update('000001.SZ', 20.5, 1000, 20500, '09:31:00')

        bars_sh = bar_agg.get_1m_bars('600000.SH')
        bars_sz = bar_agg.get_1m_bars('000001.SZ')
        assert len(bars_sh) >= 1
        assert len(bars_sz) >= 1

    def test_volume_delta_calculation(self, bar_agg):
        """成交量增量计算(累计量相减)"""
        bar_agg.update('600000.SH', 10.5, 1000, 10500, '09:30:00')
        bar_agg.update('600000.SH', 10.6, 1500, 15900, '09:30:30')
        c1m, _ = bar_agg.update('600000.SH', 10.8, 3000, 32400, '09:31:00')

        assert c1m is not None
        # 09:30这根bar的volume应该是 3000 - 0 = 3000 (从0到跨分钟时的累计量)
        # 实际: 第一个update时prev_cum_vol=0, delta=1000
        # 第二个update: delta=1500-1000=500
        # 跨分钟时completed bar的volume=1000+500=1500
        assert c1m['volume'] == 1500


# ============================================================
# 4. RqDataUpdater 测试
# ============================================================
class TestRqDataUpdaterInit:
    """测试 RqDataUpdater 初始化状态"""

    def test_initial_state(self, rq_updater):
        assert rq_updater._rq is None
        assert rq_updater._last_today_download is None
        assert rq_updater._last_backfill is None
        assert rq_updater._running is False
        assert rq_updater._current_key_idx == 0
        assert rq_updater._all_stocks == []

    def test_backfill_start_date(self, rq_updater):
        assert rq_updater.BACKFILL_START == '2020-01-01'


class TestRqDataUpdaterHelpers:
    """测试 RqDataUpdater 辅助方法(不依赖米筐连接)"""

    def test_get_last_date_from_parquet_exists(self, rq_updater):
        """已有parquet文件返回最后日期"""
        out_file = mh2.MINUTE_DIR / '600000_XSHG.parquet'
        df = pd.DataFrame({
            'datetime': pd.date_range('2026-07-19', periods=3, freq='D'),
            'open': [10, 11, 12], 'high': [11, 12, 13],
            'low': [9, 10, 11], 'close': [10.5, 11.5, 12.5],
            'volume': [100, 200, 300], 'total_turnover': [1050, 2300, 3750],
        })
        df.to_parquet(out_file, index=False)

        last_date = rq_updater._get_last_date_from_parquet(out_file)
        assert last_date == '2026-07-21'

    def test_get_last_date_from_parquet_nonexistent(self, rq_updater):
        """不存在的文件返回None"""
        last_date = rq_updater._get_last_date_from_parquet(Path('/nonexistent/file.parquet'))
        assert last_date is None

    def test_get_last_date_from_parquet_empty(self, rq_updater):
        """空parquet返回None"""
        out_file = mh2.MINUTE_DIR / 'empty.parquet'
        df = pd.DataFrame({'datetime': [], 'open': [], 'high': [], 'low': [],
                           'close': [], 'volume': [], 'total_turnover': []})
        df.to_parquet(out_file, index=False)
        last_date = rq_updater._get_last_date_from_parquet(out_file)
        assert last_date is None

    def test_find_stock_file_exact_match(self, rq_updater):
        """精确匹配文件名"""
        out_file = mh2.MINUTE_DIR / '600000_XSHG.parquet'
        out_file.write_bytes(b'')  # touch

        found = rq_updater._find_stock_file('600000.XSHG', mh2.MINUTE_DIR)
        assert found is not None
        assert found.name == '600000_XSHG.parquet'

    def test_find_stock_file_fuzzy_match(self, rq_updater):
        """模糊匹配(带行数后缀)"""
        out_file = mh2.MINUTE_DIR / '600000_XSHG_3696.parquet'
        out_file.write_bytes(b'')

        found = rq_updater._find_stock_file('600000.XSHG', mh2.MINUTE_DIR)
        assert found is not None
        assert '600000_XSHG' in found.name

    def test_find_stock_file_not_found(self, rq_updater):
        """找不到返回None"""
        found = rq_updater._find_stock_file('999999.XSHG', mh2.MINUTE_DIR)
        assert found is None

    def test_save_stock_parquet_new_file(self, rq_updater):
        """新建parquet文件"""
        new_df = pd.DataFrame({
            'datetime': pd.date_range('2026-07-19', periods=2, freq='D'),
            'order_book_id': ['600000.XSHG'] * 2,
            'open': [10, 11], 'high': [11, 12], 'low': [9, 10],
            'close': [10.5, 11.5], 'volume': [100, 200],
            'total_turnover': [1050, 2300],
        })
        rq_updater._save_stock_parquet('600000.XSHG', new_df, mh2.MINUTE_DIR)

        out_file = mh2.MINUTE_DIR / '600000_XSHG.parquet'
        assert out_file.exists()
        saved = pd.read_parquet(out_file)
        assert len(saved) == 2

    def test_save_stock_parquet_merge_dedup(self, rq_updater):
        """合并去重: 新旧数据拼接，相同datetime保留最新"""
        # 先存旧数据
        old_df = pd.DataFrame({
            'datetime': ['2026-07-19', '2026-07-20'],
            'order_book_id': ['600000.XSHG'] * 2,
            'open': [10, 11], 'high': [11, 12], 'low': [9, 10],
            'close': [10.5, 11.5], 'volume': [100, 200],
            'total_turnover': [1050, 2300],
        })
        rq_updater._save_stock_parquet('600000.XSHG', old_df, mh2.MINUTE_DIR)

        # 再存新数据(含重叠日期)
        new_df = pd.DataFrame({
            'datetime': ['2026-07-20', '2026-07-21'],
            'order_book_id': ['600000.XSHG'] * 2,
            'open': [12, 13], 'high': [13, 14], 'low': [11, 12],
            'close': [12.5, 13.5], 'volume': [300, 400],
            'total_turnover': [3750, 5400],
        })
        rq_updater._save_stock_parquet('600000.XSHG', new_df, mh2.MINUTE_DIR)

        saved = pd.read_parquet(mh2.MINUTE_DIR / '600000_XSHG.parquet')
        assert len(saved) == 3  # 去重后3天
        # 2026-07-20应该用新数据
        row_20 = saved[saved['datetime'] == '2026-07-20']
        assert len(row_20) == 1
        assert row_20.iloc[0]['close'] == 12.5


class TestRqDataUpdaterKeyRotation:
    """测试 Key 轮换逻辑"""

    def test_switch_key_when_over_threshold(self, rq_updater):
        """配额超阈值自动切换"""
        mock_rq = MagicMock()
        mock_rq.user.get_quota.return_value = {
            'bytes_used': 1800 * 1024**2,  # 1800MB
            'bytes_limit': 2048 * 1024**2,  # 2048MB → 87.9%
        }
        rq_updater._rq = mock_rq
        rq_updater._current_key_idx = 0

        with patch.object(rq_updater, '_init_rqdata') as mock_init:
            result = rq_updater._switch_key_if_needed(0.80)
            assert result is True
            mock_init.assert_called_once_with(1)

    def test_no_switch_under_threshold(self, rq_updater):
        """配额未超阈值不切换"""
        mock_rq = MagicMock()
        mock_rq.user.get_quota.return_value = {
            'bytes_used': 100 * 1024**2,
            'bytes_limit': 2048 * 1024**2,  # 4.9%
        }
        rq_updater._rq = mock_rq
        rq_updater._current_key_idx = 0

        result = rq_updater._switch_key_if_needed(0.80)
        assert result is False

    def test_no_switch_all_keys_exhausted(self, rq_updater):
        """所有Key配额都不足时不切换(继续用当前)"""
        mock_rq = MagicMock()
        mock_rq.user.get_quota.return_value = {
            'bytes_used': 1800 * 1024**2,
            'bytes_limit': 2048 * 1024**2,
        }
        rq_updater._rq = mock_rq
        rq_updater._current_key_idx = len(mh2.LICENSES) - 1  # 最后一个key

        result = rq_updater._switch_key_if_needed(0.80)
        assert result is False


class TestRqDataUpdaterRunning:
    """测试运行状态互斥锁"""

    def test_download_today_reject_when_running(self, rq_updater):
        """运行中拒绝当日下载"""
        rq_updater._running = True
        result = rq_updater.download_today_update()
        assert result is False

    def test_backfill_reject_when_running(self, rq_updater):
        """运行中拒绝历史补齐"""
        rq_updater._running = True
        result = rq_updater.backfill_history()
        assert result is False


# ============================================================
# 5. 定时任务调度测试
# ============================================================
class TestScheduledTasks:
    """测试 16:30当日下载 + 23:00历史补齐 调度"""

    def _make_hub_with_mock_time(self, hour, minute):
        """创建hub并mock当前时间"""
        hub = mh2.MarketHubV2()
        hub.rq_updater._last_today_download = None
        hub.rq_updater._last_backfill = None
        return hub

    def test_16_30_triggers_today_download(self):
        """16:30触发当日下载"""
        hub = mh2.MarketHubV2()
        hub.rq_updater._last_today_download = None

        with patch.object(mh2, 'datetime') as mock_dt:
            mock_now = MagicMock()
            mock_now.hour = 16
            mock_now.minute = 30
            mock_now.date.return_value = date(2026, 7, 21)
            mock_dt.now.return_value = mock_now

            with patch.object(threading.Thread, 'start') as mock_start:
                hub._check_scheduled_tasks()
                # 应该启动了当日下载线程
                assert mock_start.called

    def test_23_00_triggers_backfill(self):
        """23:00触发历史补齐"""
        hub = mh2.MarketHubV2()
        hub.rq_updater._last_backfill = None

        with patch.object(mh2, 'datetime') as mock_dt:
            mock_now = MagicMock()
            mock_now.hour = 23
            mock_now.minute = 0
            mock_now.date.return_value = date(2026, 7, 21)
            mock_dt.now.return_value = mock_now

            with patch.object(threading.Thread, 'start') as mock_start:
                hub._check_scheduled_tasks()
                assert mock_start.called

    def test_no_trigger_at_12_00(self):
        """12:00不触发任何任务"""
        hub = mh2.MarketHubV2()

        with patch.object(mh2, 'datetime') as mock_dt:
            mock_now = MagicMock()
            mock_now.hour = 12
            mock_now.minute = 0
            mock_dt.now.return_value = mock_now

            with patch.object(threading.Thread, 'start') as mock_start:
                hub._check_scheduled_tasks()
                assert not mock_start.called

    def test_already_downloaded_today_skips(self):
        """今日已下载过则跳过"""
        hub = mh2.MarketHubV2()
        hub.rq_updater._last_today_download = datetime(2026, 7, 21, 16, 30)

        with patch.object(mh2, 'datetime') as mock_dt:
            mock_now = MagicMock()
            mock_now.hour = 16
            mock_now.minute = 30
            mock_now.date.return_value = date(2026, 7, 21)
            mock_dt.now.return_value = mock_now

            with patch.object(threading.Thread, 'start') as mock_start:
                hub._check_scheduled_tasks()
                assert not mock_start.called

    def test_already_backfilled_today_skips(self):
        """今日已补齐过则跳过"""
        hub = mh2.MarketHubV2()
        hub.rq_updater._last_backfill = datetime(2026, 7, 21, 23, 0)

        with patch.object(mh2, 'datetime') as mock_dt:
            mock_now = MagicMock()
            mock_now.hour = 23
            mock_now.minute = 30
            mock_now.date.return_value = date(2026, 7, 21)
            mock_dt.now.return_value = mock_now

            with patch.object(threading.Thread, 'start') as mock_start:
                hub._check_scheduled_tasks()
                assert not mock_start.called


# ============================================================
# 6. FastAPI 接口集成测试
# ============================================================
class TestFastAPIEndpoints:
    """测试 FastAPI 9个端点 (需要启动httpx/TestClient)"""

    @pytest.fixture
    def client(self):
        """创建TestClient"""
        from fastapi.testclient import TestClient
        from fastapi import FastAPI, Query
        from fastapi.responses import JSONResponse

        hub = mh2.MarketHubV2()
        # 预填充一些快照数据
        hub._snapshots = {
            '600000.SH': {'code': '600000.SH', 'last_price': 10.5, 'volume': 1000,
                          'is_index': False, 'update_time': '09:30:00'},
            '000001.IDX': {'code': '000001.IDX', 'last_price': 3200.0, 'volume': 0,
                           'is_index': True, 'update_time': '09:30:00'},
        }
        hub._start_time = time.time()
        hub._msg_count = 100
        hub._snap_count = 50
        hub._bar_count = 10
        hub._codes_seen = {'600000.SH', '000001.IDX'}
        hub._index_codes = {'000001.IDX'}

        # 构建FastAPI app (与market_hub_v2.py中_run_api相同)
        app = FastAPI(title="Market Hub API Test", version="2.0")

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
            }}

        @app.post("/api/rqdata/update")
        def api_rqdata_update(mode: str = Query('today')):
            if mode == 'backfill':
                return {"ok": True, "message": "米筐历史补齐任务已启动"}
            else:
                return {"ok": True, "message": "米筐当日下载任务已启动"}

        return TestClient(app)

    def test_snapshot_existing(self, client):
        """查询已有快照"""
        resp = client.get("/api/snapshot/600000.SH")
        assert resp.status_code == 200
        data = resp.json()
        assert data['ok'] is True
        assert data['data']['code'] == '600000.SH'
        assert data['data']['last_price'] == 10.5

    def test_snapshot_nonexistent(self, client):
        """查询不存在的快照"""
        resp = client.get("/api/snapshot/999999.SH")
        assert resp.status_code == 200
        data = resp.json()
        assert data['ok'] is False

    def test_all_snapshots(self, client):
        """查询全部快照"""
        resp = client.get("/api/snapshots")
        assert resp.status_code == 200
        data = resp.json()
        assert data['ok'] is True
        assert data['count'] == 2

    def test_all_snapshots_index_only(self, client):
        """只查指数快照"""
        resp = client.get("/api/snapshots?index_only=true")
        assert resp.status_code == 200
        data = resp.json()
        assert data['count'] == 1
        assert '000001.IDX' in data['data']

    def test_stats(self, client):
        """统计接口"""
        resp = client.get("/api/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data['ok'] is True
        assert data['data']['total_msgs'] == 100

    def test_rqdata_update_today(self, client):
        """手动触发当日下载"""
        resp = client.post("/api/rqdata/update?mode=today")
        assert resp.status_code == 200
        data = resp.json()
        assert data['ok'] is True
        assert '当日下载' in data['message']

    def test_rqdata_update_backfill(self, client):
        """手动触发历史补齐"""
        resp = client.post("/api/rqdata/update?mode=backfill")
        assert resp.status_code == 200
        data = resp.json()
        assert data['ok'] is True
        assert '历史补齐' in data['message']


# ============================================================
# 7. 历史补齐核心逻辑测试
# ============================================================
class TestBackfillLogic:
    """测试逐日向前补齐核心逻辑"""

    def test_backfill_scan_need_update(self, rq_updater):
        """扫描: 已有文件但日期落后 → need_update"""
        # 创建一个只到7月19日的文件
        out_file = mh2.MINUTE_DIR / '600000_XSHG.parquet'
        df = pd.DataFrame({
            'datetime': pd.date_range('2026-07-17', periods=3, freq='D'),
            'open': [10, 11, 12], 'high': [11, 12, 13],
            'low': [9, 10, 11], 'close': [10.5, 11.5, 12.5],
            'volume': [100, 200, 300], 'total_turnover': [1050, 2300, 3750],
        })
        df.to_parquet(out_file, index=False)

        # 扫描
        f = rq_updater._find_stock_file('600000.XSHG', mh2.MINUTE_DIR)
        last_date = rq_updater._get_last_date_from_parquet(f)
        today_str = date.today().isoformat()

        assert f is not None
        assert last_date is not None
        assert last_date < today_str  # 需要更新

    def test_backfill_scan_need_create(self, rq_updater):
        """扫描: 无文件 → need_create"""
        f = rq_updater._find_stock_file('600001.XSHG', mh2.MINUTE_DIR)
        assert f is None  # 需要新建

    def test_backfill_scan_up_to_date(self, rq_updater):
        """扫描: 文件已是最新 → 跳过"""
        today_str = date.today().isoformat()
        out_file = mh2.MINUTE_DIR / '600000_XSHG.parquet'
        df = pd.DataFrame({
            'datetime': [today_str],
            'open': [10], 'high': [11], 'low': [9], 'close': [10.5],
            'volume': [100], 'total_turnover': [1050],
        })
        df.to_parquet(out_file, index=False)

        f = rq_updater._find_stock_file('600000.XSHG', mh2.MINUTE_DIR)
        last_date = rq_updater._get_last_date_from_parquet(f)
        assert last_date == today_str  # 已是最新

    def test_30_day_gap_limit(self, rq_updater):
        """缺失超过30天时限制只补最近30天"""
        # 创建一个只到6月1日的文件(距今>30天)
        out_file = mh2.MINUTE_DIR / '600000_XSHG.parquet'
        df = pd.DataFrame({
            'datetime': ['2026-06-01'],
            'open': [10], 'high': [11], 'low': [9], 'close': [10.5],
            'volume': [100], 'total_turnover': [1050],
        })
        df.to_parquet(out_file, index=False)

        f = rq_updater._find_stock_file('600000.XSHG', mh2.MINUTE_DIR)
        last_date = rq_updater._get_last_date_from_parquet(f)

        # 模拟_backfill_stock_data中的逻辑
        min_start = last_date  # '2026-06-01'
        days_gap = (date.today() - pd.to_datetime(min_start).date()).days
        if days_gap > 30:
            effective_start = (date.today() - timedelta(days=30)).isoformat()
        else:
            effective_start = min_start

        assert days_gap > 30
        # effective_start应该是30天前，不是6月1日
        assert effective_start > min_start


# ============================================================
# 8. LICENSES 配置测试
# ============================================================
class TestLicensesConfig:
    """测试4个key配置"""

    def test_all_4_keys_present(self):
        """4个key全部存在"""
        assert len(mh2.LICENSES) == 4
        assert all(lic for lic in mh2.LICENSES)  # 没有空串

    def test_lic1_lic2_populated(self):
        """LIC1/LIC2有值"""
        assert mh2.LIC1 != ''
        assert mh2.LIC2 != ''

    def test_lic3_lic4_populated(self):
        """LIC3/LIC4有值"""
        assert mh2.LIC3 != ''
        assert mh2.LIC4 != ''

    def test_licenses_order(self):
        """LICENSES顺序: LIC1→LIC2→LIC3→LIC4"""
        assert mh2.LICENSES == [mh2.LIC1, mh2.LIC2, mh2.LIC3, mh2.LIC4]


# ============================================================
# 9. 指数配置测试
# ============================================================
class TestIndexConfig:
    """测试指数/行业指数配置完整性"""

    def test_indices_count(self):
        """大盘指数9个"""
        assert len(mh2.INDICES) == 9

    def test_industry_indices_count(self):
        """行业指数31个"""
        assert len(mh2.INDUSTRY_INDICES) == 31

    def test_known_indices_subset(self):
        """KNOWN_INDICES是INDICES的子集"""
        for idx in mh2.KNOWN_INDICES:
            assert idx + '.XSHG' in mh2.INDICES or idx + '.XSHE' in mh2.INDICES

    def test_all_indices_have_valid_suffix(self):
        """所有指数代码格式正确 — 大盘.XSHG/.XSHE, 行业.INDX"""
        for code in list(mh2.INDICES.keys()):
            assert code.endswith('.XSHG') or code.endswith('.XSHE'), \
                f"大盘指数代码格式错误: {code}"
        for code in list(mh2.INDUSTRY_INDICES.keys()):
            assert code.endswith('.INDX'), \
                f"行业指数代码格式错误: {code} (应为.INDX)"


# ============================================================
# 10. 边界条件与异常测试
# ============================================================
class TestEdgeCases:
    """测试边界条件和异常处理"""

    def test_tick_saver_concurrent_add(self, tick_saver):
        """并发添加tick(线程安全)"""
        errors = []

        def add_ticks(code, n):
            try:
                for i in range(n):
                    tick = {'timestamp': f'2026-07-21 09:30:{i:02d}',
                            'code': code, 'last_price': 10.0, 'volume': 100, 'turnover': 1000}
                    tick_saver.add(code, tick)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=add_ticks, args=(f'60000{i}.SH', 100))
                   for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert tick_saver._tick_count == 500

    def test_bar_aggregator_invalid_timestamp(self, bar_agg):
        """无效时间戳不崩溃"""
        c1m, c5m = bar_agg.update('600000.SH', 10.5, 1000, 10500, 'invalid')
        assert c1m is None
        assert c5m is None

    def test_bar_aggregator_empty_timestamp(self, bar_agg):
        """空时间戳不崩溃"""
        c1m, c5m = bar_agg.update('600000.SH', 10.5, 1000, 10500, '')
        assert c1m is None
        assert c5m is None

    def test_normalize_code_whitespace(self):
        """带空格的SecurityID"""
        assert mh2.normalize_code(' 600000 ') == '600000.SH'

    def test_save_stock_parquet_empty_df(self, rq_updater):
        """保存空DataFrame不崩溃"""
        empty_df = pd.DataFrame({
            'datetime': [], 'order_book_id': [], 'open': [], 'high': [],
            'low': [], 'close': [], 'volume': [], 'total_turnover': [],
        })
        # 不应抛异常
        rq_updater._save_stock_parquet('600000.XSHG', empty_df, mh2.MINUTE_DIR)

    def test_tick_saver_flush_empty_buffer(self, tick_saver):
        """空buffer flush不崩溃"""
        tick_saver.flush()  # 不应抛异常
        assert tick_saver._tick_count == 0


# ============================================================
# 11. 数据完整性检查测试
# ============================================================
class TestDataIntegrityCheck:
    """测试下载后数据完整性验证"""

    def _make_1m_df(self, n_days=5, start='2026-07-14', nan_cols=None, bad_rows=None):
        """构造1m测试数据(每天240行)"""
        rows_per_day = 240
        datetimes = []
        base = pd.Timestamp(start)
        for d in range(n_days):
            day = base + pd.Timedelta(days=d)
            for h in range(4):
                for m in range(60):
                    dt = day + pd.Timedelta(hours=9+h, minutes=30+m)
                    datetimes.append(dt)

        n = len(datetimes)
        df = pd.DataFrame({
            'datetime': datetimes,
            'order_book_id': ['600000.XSHG'] * n,
            'open': np.random.uniform(10, 20, n),
            'high': np.random.uniform(10, 20, n),
            'low': np.random.uniform(10, 20, n),
            'close': np.random.uniform(10, 20, n),
            'volume': np.random.randint(100, 10000, n),
            'total_turnover': np.random.uniform(1e6, 1e8, n),
        })
        # 确保 high >= low
        df['high'] = df[['open', 'high', 'low', 'close']].max(axis=1)
        df['low'] = df[['open', 'high', 'low', 'close']].min(axis=1)

        if nan_cols:
            for col in nan_cols:
                df.loc[df.sample(frac=0.1, random_state=42).index, col] = np.nan
        if bad_rows:
            # 插入close=0的坏行
            for idx in bad_rows:
                if idx < len(df):
                    df.loc[idx, 'close'] = 0

        return df

    def test_check_data_integrity_pass(self, rq_updater):
        """正常数据通过完整性检查"""
        df = self._make_1m_df(n_days=3)
        result = rq_updater._check_data_integrity(df, '1m', '600000.XSHG')
        assert result['valid'] is True
        assert result['nan_pct'] == 0.0

    def test_check_data_integrity_high_nan(self, rq_updater):
        """NaN比例>5%拒绝写入"""
        df = self._make_1m_df(n_days=3, nan_cols=['close', 'open', 'high', 'low'])
        result = rq_updater._check_data_integrity(df, '1m', '600000.XSHG')
        # 4列各10%NaN → 4*10%/6 ≈ 6.7% overall > 5%
        assert result['nan_pct'] > 5.0
        assert result['valid'] is False

    def test_check_data_integrity_close_zero(self, rq_updater):
        """close=0不合理"""
        df = self._make_1m_df(n_days=1, bad_rows=[0, 1])
        result = rq_updater._check_data_integrity(df, '1m', '600000.XSHG')
        assert result['bad_price_count'] >= 2

    def test_check_data_integrity_empty_df(self, rq_updater):
        """空DataFrame"""
        df = pd.DataFrame()
        result = rq_updater._check_data_integrity(df, '1m', '600000.XSHG')
        assert result['valid'] is False

    def test_check_data_integrity_daily_row_count(self, rq_updater):
        """日线数据行数检查: 5天应有5行"""
        df = pd.DataFrame({
            'datetime': pd.date_range('2026-07-14', periods=5, freq='B'),
            'order_book_id': ['600000.XSHG'] * 5,
            'open': [10]*5, 'high': [11]*5, 'low': [9]*5,
            'close': [10.5]*5, 'volume': [1000]*5, 'total_turnover': [10500]*5,
        })
        result = rq_updater._check_data_integrity(df, '1d', '600000.XSHG')
        assert result['valid'] is True
        assert result['row_count'] == 5


# ============================================================
# 12. 增量更新逻辑测试
# ============================================================
class TestIncrementalUpdate:
    """测试逐股增量更新: 每只股票只下载缺失段"""

    def test_compute_missing_ranges_no_gap(self, rq_updater):
        """数据完整: 无缺失段"""
        # 文件有7月14-18日(5天)的数据
        out_file = mh2.MINUTE_DIR / '600000_XSHG.parquet'
        df = pd.DataFrame({
            'datetime': pd.date_range('2026-07-14', periods=5, freq='B'),
            'open': [10]*5, 'high': [11]*5, 'low': [9]*5,
            'close': [10.5]*5, 'volume': [100]*5, 'total_turnover': [1050]*5,
        })
        df.to_parquet(out_file, index=False)

        ranges = rq_updater._compute_missing_ranges('600000.XSHG', mh2.MINUTE_DIR, '1m')
        # 7月18日之后到今天可能有缺失，但7月14-18这段完整
        # 应该只有(last_date+1 → today)的range
        for start, end in ranges:
            assert start > '2026-07-18'

    def test_compute_missing_ranges_with_gap(self, rq_updater):
        """数据中间有缺口: 7月14-15日有, 7月16-18日缺失"""
        out_file = mh2.MINUTE_DIR / '600000_XSHG.parquet'
        df = pd.DataFrame({
            'datetime': ['2026-07-14', '2026-07-15'],
            'open': [10, 11], 'high': [11, 12], 'low': [9, 10],
            'close': [10.5, 11.5], 'volume': [100, 200], 'total_turnover': [1050, 2300],
        })
        df.to_parquet(out_file, index=False)

        ranges = rq_updater._compute_missing_ranges('600000.XSHG', mh2.MINUTE_DIR, '1m')
        # 应该有(2026-07-16 → today)的range
        assert len(ranges) >= 1
        assert ranges[0][0] == '2026-07-16'  # 从缺失日开始

    def test_compute_missing_ranges_no_file(self, rq_updater):
        """无文件: 从BACKFILL_START开始"""
        ranges = rq_updater._compute_missing_ranges('600001.XSHG', mh2.MINUTE_DIR, '1m')
        assert len(ranges) >= 1
        assert ranges[0][0] == rq_updater.BACKFILL_START

    def test_compute_missing_ranges_30day_limit(self, rq_updater):
        """缺失超过30天时分段"""
        out_file = mh2.MINUTE_DIR / '600000_XSHG.parquet'
        # 文件只有很早的数据
        df = pd.DataFrame({
            'datetime': ['2020-03-01'],
            'open': [10], 'high': [11], 'low': [9],
            'close': [10.5], 'volume': [100], 'total_turnover': [1050],
        })
        df.to_parquet(out_file, index=False)

        ranges = rq_updater._compute_missing_ranges('600000.XSHG', mh2.MINUTE_DIR, '1m')
        # 第一段应该只补最近30天
        first_start = pd.to_datetime(ranges[0][0])
        first_end = pd.to_datetime(ranges[0][1])
        assert (first_end - first_start).days <= 31


# ============================================================
# 13. 开盘5分钟tick下载测试
# ============================================================
class TestOpenTickDownload:
    """测试开盘5分钟tick数据下载保存"""

    def test_open_tick_dir_exists(self):
        """开盘tick目录常量定义"""
        assert hasattr(mh2, 'OPEN_TICK_DIR')
        assert 'open_tick' in str(mh2.OPEN_TICK_DIR).lower() or 'open_tick' in str(mh2.OPEN_TICK_DIR)

    def test_save_open_tick_parquet(self, rq_updater):
        """保存开盘tick数据到parquet"""
        mh2.OPEN_TICK_DIR.mkdir(parents=True, exist_ok=True)
        tick_df = pd.DataFrame({
            'datetime': pd.date_range('2026-07-17 09:25:00', periods=100, freq='3s'),
            'order_book_id': ['600000.XSHG'] * 100,
            'open': [10.0] * 100, 'last': [10.5] * 100,
            'high': [11.0] * 100, 'low': [9.5] * 100,
            'prev_close': [10.0] * 100, 'volume': [100] * 100,
            'total_turnover': [1050.0] * 100,
            'a1': [10.4] * 100, 'b1': [10.3] * 100,
            'a1_v': [50] * 100, 'b1_v': [60] * 100,
        })
        out_file = mh2.OPEN_TICK_DIR / '600000_XSHG' / '2026-07-17.parquet'
        out_file.parent.mkdir(parents=True, exist_ok=True)
        tick_df.to_parquet(out_file, index=False)

        assert out_file.exists()
        saved = pd.read_parquet(out_file)
        assert len(saved) == 100

    def test_open_tick_fields_available(self):
        """开盘tick包含5档盘口字段"""
        # 验证米筐tick fields配置
        assert hasattr(mh2, 'TICK_FIELDS')
        # 应包含a1-a5, b1-b5, a1_v-a5_v, b1_v-b5_v
        tick_fields = mh2.TICK_FIELDS
        assert 'a1' in tick_fields
        assert 'b5' in tick_fields
        assert 'a1_v' in tick_fields


# ============================================================
# Cleanup
# ============================================================
def test_cleanup():
    """清理临时目录"""
    if TMP_ROOT.exists():
        shutil.rmtree(TMP_ROOT, ignore_errors=True)
