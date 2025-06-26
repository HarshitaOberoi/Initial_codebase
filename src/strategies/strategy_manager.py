import yaml
import pandas as pd
import asyncio
import time
from src.utils.logger import setup_logger
from src.strategies.scalping import ScalpingStrategy
from src.strategies.swing import SwingStrategy
from src.strategies.long_swing import LongSwingStrategy
from src.strategies.trend import TrendStrategy
from src.data.indicators import Indicators
from src.exchange.coindcx_client import to_api_symbol

logger = setup_logger()

class StrategyManager:
    def __init__(self, config_input="config/config.yaml"):
        try:
            if isinstance(config_input, str):
                with open(config_input, 'r') as file:
                    self.config = yaml.safe_load(file)
            elif isinstance(config_input, dict):
                self.config = config_input
            else:
                raise ValueError("config_input must be a file path or dictionary")
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise
        self.enabled_strategies = self.config.get('strategies', {}).get('enabled', ['scalping', 'swing', 'long_swing', 'trend'])
        self.strategies = {}
        self.indicators = Indicators(self.config)
        self.exchange_rate = self.config.get('trading', {}).get('exchange_rate_usdt_inr', 86)
        self.executor = None
        self.fetcher = None
        self.last_signal_time = {}
        self.initialize_strategies()
        logger.info(f"StrategyManager initialized with enabled strategies: {self.enabled_strategies}")

    def set_executor(self, executor):
        self.executor = executor
        logger.info("Executor set for StrategyManager")

    def set_fetcher(self, fetcher):
        self.fetcher = fetcher
        logger.info("Fetcher set for StrategyManager")
    def set_executor(self, executor):
        self.executor = executor
        logger.info("Executor set for StrategyManager")

    def initialize_strategies(self):
        strategy_map = {
            'scalping': ScalpingStrategy,
            'swing': SwingStrategy,
            'long_swing': LongSwingStrategy,
            'trend': TrendStrategy
        }
        for strategy_name in self.enabled_strategies:
            if strategy_name in strategy_map:
                try:
                    self.strategies[strategy_name] = strategy_map[strategy_name](self.config)
                    logger.info(f"Initialized {strategy_name} strategy")
                except Exception as e:
                    logger.debug(f"Failed to initialize {strategy_name} strategy: {e}")
            else:
                logger.debug(f"Unknown strategy: {strategy_name}")

    async def process_data(self, symbol, timeframe, data, order_book=None):
        try:
            if data is None or data.empty:
                logger.debug(f"No data for {symbol} ({timeframe})")
                return None
            if not self.executor:
                logger.debug(f"No executor defined for {symbol} ({timeframe})")
                return None
            api_symbol = to_api_symbol(symbol, is_futures=True)
            if api_symbol is None or '__' in api_symbol:
                logger.error(f"Invalid symbol {symbol} (API: {api_symbol})")
                return None
            if self.fetcher and api_symbol not in self.fetcher.valid_pairs:
                logger.error(f"Invalid symbol {api_symbol}. Valid pairs: {list(self.fetcher.valid_pairs)[:10]}")
                return None
            buffer_key = f"{symbol}_{timeframe}"
            if buffer_key in self.last_signal_time and (time.time() - self.last_signal_time[buffer_key]) < 60:
                logger.debug(f"Skipping signal for {buffer_key}: Cooldown active")
                return None
            balance = self.executor.get_balance() if self.executor else 0
            open_trades = self.executor.get_open_trades() if self.executor else []
            logger.debug(f"Processing data for {symbol} ({timeframe}): Balance={balance}, Open trades={len(open_trades)}, Order book={bool(order_book)}")
            signal, params = await self.evaluate_strategies(data, symbol, timeframe, balance, None, open_trades, order_book)
            if signal != 'hold' and signal in ['buy', 'sell']:
                logger.info(f"Executing {signal} trade for {api_symbol} ({timeframe}): {params}")
                await self.executor.execute_trade(api_symbol, timeframe, signal, params, params.get('strategy', 'unknown'))
                self.last_signal_time[buffer_key] = time.time()
            return signal, params
        except Exception as e:
            logger.error(f"Error processing data for {symbol} ({timeframe}): {e}", exc_info=True)
            return None

    async def evaluate_strategies(self, data, symbol, timeframe, balance, trade=None, open_trades=None, order_book=None):
        try:
            if data is None or data.empty:
                logger.debug(f"Empty data for {symbol} ({timeframe})")
                return 'hold', {}
            logger.debug(f"Data for {symbol} ({timeframe}): Rows={len(data)}")
            data_with_indicators = self.indicators.calculate_all(data)
            if data_with_indicators.empty:
                logger.debug(f"Failed to compute indicators for {symbol} ({timeframe})")
                return 'hold', {}
            signals = []
            for strategy_name, strategy in self.strategies.items():
                try:
                    signal, params = strategy.evaluate(data_with_indicators, symbol, timeframe, balance, trade, open_trades, order_book)
                    if signal != 'hold':
                        params['strategy'] = strategy_name
                        signals.append((signal, params, strategy_name))
                        logger.info(f"Signal from {strategy_name} for {symbol} ({timeframe}): {signal}, Params: {params}")
                except Exception as e:
                    logger.debug(f"Error evaluating strategy {strategy_name} for {symbol} ({timeframe}): {e}")
            if not signals:
                logger.debug(f"No signals generated for {symbol} ({timeframe})")
                return 'hold', {'reason': 'No strategy triggered'}
            return await self._select_strongest_signal(signals, data_with_indicators, symbol, timeframe)
        except Exception as e:
            logger.error(f"Error evaluating strategies for {symbol} ({timeframe}): {e}", exc_info=True)
            return 'hold', {'reason': str(e)}

    async def _select_strongest_signal(self, signals, data, symbol, timeframe):
        try:
            if len(signals) == 1:
                logger.info(f"Single signal for {symbol} ({timeframe}): {signals[0][0]} from {signals[0][2]}")
                return signals[0][0], signals[0][1]
            latest = data.iloc[-1]
            signal_scores = []
            for signal, params, strategy_name in signals:
                score = 0
                if latest.get('adx', 25) > 20:
                    score += 1
                if latest.get('volume', 0) > latest.get('volume_sma', 0):
                    score += 1
                if strategy_name == 'trend' and latest.get('adx', 25) > 25:
                    score += 1
                if strategy_name == 'scalping' and timeframe in ['1m', '5m']:
                    score += 1
                signal_scores.append((signal, params, score))
                logger.debug(f"Signal score for {strategy_name} ({signal}) on {symbol} ({timeframe}): {score}")
            signal_scores.sort(key=lambda x: x[2], reverse=True)
            top_signal, top_params, top_score = signal_scores[0]
            logger.info(f"Top signal for {symbol} ({timeframe}): {top_signal} from {top_params.get('strategy')} with score {top_score}")
            if top_signal in ['buy', 'sell'] and self.fetcher:
                higher_tf = self._get_higher_timeframe(timeframe)
                if higher_tf:
                    higher_data = await self._fetch_higher_tf_data(symbol, higher_tf)
                    if higher_data is not None and not higher_data.empty:
                        higher_data = self.indicators.calculate_all(higher_data)
                        adx_value = higher_data.iloc[-1].get('adx', 25)
                        logger.debug(f"Higher TF ADX for {symbol} ({higher_tf}): {adx_value}")
            return top_signal, top_params
        except Exception as e:
            logger.error(f"Error selecting signal for {symbol} ({timeframe}): {e}", exc_info=True)
            return 'hold', {}

    def _get_higher_timeframe(self, timeframe):
        tf_map = {'1m': '5m', '5m': '1h', '1h': '4h', '4h': '1d'}
        tf = tf_map.get(timeframe)
        if not tf:
            logger.debug(f"Unsupported timeframe {timeframe}, no higher timeframe")
        return tf

    async def _fetch_higher_tf_data(self, symbol, timeframe):
        try:
            if not self.fetcher:
                logger.error(f"No fetcher defined for higher timeframe data for {symbol} ({timeframe})")
                return None
            data = await self.fetcher.fetch_data(symbol, timeframe, limit=50)
            return data
        except Exception as e:
            logger.error(f"Error fetching higher timeframe data for {symbol} ({timeframe}): {e}", exc_info=True)
            return None