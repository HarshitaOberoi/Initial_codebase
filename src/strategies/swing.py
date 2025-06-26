import pandas as pd
from src.utils.logger import setup_logger
from src.exchange.coindcx_client import to_api_symbol

logger = setup_logger()

class SwingStrategy:
    def __init__(self, config):
        self.config = config
        self.rsi_period = config.get('strategies', {}).get('swing', {}).get('rsi_period', 14)
        self.bollinger_period = config.get('strategies', {}).get('swing', {}).get('bollinger_period', 20)
        self.min_rsi = config.get('strategies', {}).get('swing', {}).get('min_rsi', 40)
        self.rsi_overbought = config.get('strategies', {}).get('swing', {}).get('rsi_overbought', 60)
        self.leverage = config.get('strategies', {}).get('swing', {}).get('leverage', 3)
        self.risk_reward_ratio = config.get('trading', {}).get('risk_reward_ratio', {}).get('swing', 2.0)
        self.exchange_rate = config.get('trading', {}).get('exchange_rate_usd_inr', 86)
        self.adx_period = config.get('strategies', {}).get('adx_period', 14)
        self.max_spread = config.get('strategies', {}).get('swing', {}).get('max_spread', 0.001)  # Max bid-ask spread (0.1%)
        logger.info("SwingStrategy initialized with relaxed thresholds")

    def evaluate(self, data, symbol, timeframe, balance, trade, open_trades=None, order_book=None):
        try:
            if data is None or data.empty or len(data) < max(self.rsi_period, self.bollinger_period):
                logger.warning(f"Insufficient data for {symbol} ({timeframe})")
                return 'hold', {}
            latest = data.iloc[-1]
            required_indicators = ['rsi', 'bollinger_mid', 'atr', 'adx', 'volume', 'volume_sma']
            if not all(ind in latest for ind in required_indicators):
                missing = [ind for ind in required_indicators if ind not in latest]
                logger.warning(f"Missing indicators for {symbol} ({timeframe}): {missing}")
                return 'hold', {}

            # Check order book for liquidity
            spread = None
            if order_book and order_book.get('bids') and order_book.get('asks'):
                bid = order_book['bids'][0][0] if order_book['bids'] else latest['close']
                ask = order_book['asks'][0][0] if order_book['asks'] else latest['close']
                spread = (ask - bid) / bid if bid > 0 else float('inf')
                logger.debug(f"Order book for {symbol} ({timeframe}): Bid={bid:.4f}, Ask={ask:.4f}, Spread={spread:.4%}")
                if spread > self.max_spread:
                    logger.warning(f"Spread too wide for {symbol} ({timeframe}): {spread:.4%}")
                    return 'hold', {'reason': 'Spread too wide'}

            logger.debug(f"Swing for {symbol} ({timeframe}): RSI={latest['rsi']:.2f}, ADX={latest['adx']:.2f}")
            logger.debug(f"Last 10 RSI: {data['rsi'].tail(10).to_list()}, ADX: {data['adx'].tail(10).to_list()}")

            api_symbol = to_api_symbol(symbol, is_futures=True)
            has_open_buy = any(
                t.get('symbol') == api_symbol and t.get('timeframe') == timeframe and t.get('signal') == 'buy' and t.get('status') == 'open'
                for t in open_trades or []
            )
            has_open_sell = any(
                t.get('symbol') == api_symbol and t.get('timeframe') == timeframe and t.get('signal') == 'sell' and t.get('status') == 'open'
                for t in open_trades or []
            )
            logger.debug(f"Open trades for {api_symbol} ({timeframe}): Buy={has_open_buy}, Sell={has_open_sell}")

            entry_price = latest['close']
            risk_per_trade = self.config.get('trading', {}).get('risk_per_trade', {}).get(timeframe, 0.025)
            atr_factor = 1.0
            stop_loss_distance = latest['atr'] * atr_factor
            risk_per_unit = stop_loss_distance
            quantity = (balance * risk_per_trade) / risk_per_unit if risk_per_unit > 0 else 0.01
            quantity = max(round(quantity, 8), 0.001)  # Ensure min quantity

            # Buy signal
            if not has_open_buy and not has_open_sell and latest['rsi'] <= self.min_rsi and latest['close'] <= latest['bollinger_mid']:
                stop_loss = entry_price - stop_loss_distance
                take_profit = entry_price + stop_loss_distance * self.risk_reward_ratio
                params = {
                    'entry_price': float(entry_price),
                    'quantity': float(quantity),
                    'take_profit': float(take_profit),
                    'stop_loss': float(stop_loss),
                    'leverage': float(self.leverage),
                    'strategy': 'swing'
                }
                logger.info(f"Buy signal for {symbol} ({timeframe}): {params}")
                return 'buy', params

            # Sell signal
            if not has_open_buy and not has_open_sell and latest['rsi'] >= self.rsi_overbought and latest['close'] >= latest['bollinger_mid']:
                stop_loss = entry_price + stop_loss_distance
                take_profit = entry_price - stop_loss_distance * self.risk_reward_ratio
                params = {
                    'entry_price': float(entry_price),
                    'quantity': float(quantity),
                    'take_profit': float(take_profit),
                    'stop_loss': float(stop_loss),
                    'leverage': float(self.leverage),
                    'strategy': 'swing'
                }
                logger.info(f"Sell signal for {symbol} ({timeframe}): {params}")
                return 'sell', params

            # Close buy position
            if has_open_buy and latest['rsi'] >= self.rsi_overbought:
                stop_loss = entry_price + stop_loss_distance
                take_profit = entry_price - stop_loss_distance * self.risk_reward_ratio
                params = {
                    'entry_price': float(entry_price),
                    'quantity': float(quantity),
                    'take_profit': float(take_profit),
                    'stop_loss': float(stop_loss),
                    'leverage': float(self.leverage),
                    'strategy': 'swing'
                }
                logger.info(f"Close buy signal for {symbol} ({timeframe}): {params}")
                return 'sell', params

            # Close sell position
            if has_open_sell and latest['rsi'] <= self.min_rsi:
                stop_loss = entry_price - stop_loss_distance
                take_profit = entry_price + stop_loss_distance * self.risk_reward_ratio
                params = {
                    'entry_price': float(entry_price),
                    'quantity': float(quantity),
                    'take_profit': float(take_profit),
                    'stop_loss': float(stop_loss),
                    'leverage': float(self.leverage),
                    'strategy': 'swing'
                }
                logger.info(f"Close sell signal for {symbol} ({timeframe}): {params}")
                return 'buy', params

            logger.debug(f"No signal for {symbol} ({timeframe})")
            return 'hold', {}
        except Exception as e:
            logger.error(f"Error in SwingStrategy for {symbol} ({timeframe}): {e}")
            return 'hold', {}