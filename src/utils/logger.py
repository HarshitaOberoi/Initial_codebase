import logging
import os
from logging.handlers import RotatingFileHandler
import yaml
from datetime import datetime

def setup_logger():
    logger = logging.getLogger('TradingBot')
    # Force DEBUG logging for troubleshooting
    logger.setLevel(logging.DEBUG)

    if not os.path.exists('logs'):
        os.makedirs('logs')

    config_path = 'config/config.yaml'
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        suppress_candle_logs = config.get('logging', {}).get('suppress_candle_logs', False)
        log_candles = config.get('logging', {}).get('log_candles', True)
    except Exception as e:
        logger.error(f"Failed to load config for logging: {e}")
        suppress_candle_logs = False
        log_candles = True

    class CandleFilter(logging.Filter):
        def filter(self, record):
            if ('Fetching data for' in record.getMessage() or 'Kline for' in record.getMessage()):
                return log_candles and not suppress_candle_logs
            return True

    # General log file
    file_handler = RotatingFileHandler('logs/trading_bot.log', maxBytes=1000000, backupCount=5)
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s.%(funcName)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    file_handler.addFilter(CandleFilter())

    # Candlestick-specific log file
    candle_file_handler = RotatingFileHandler('logs/candlestick.log', maxBytes=1000000, backupCount=5)
    candle_file_handler.setLevel(logging.DEBUG)
    candle_file_handler.setFormatter(file_formatter)
    class KlineOnlyFilter(logging.Filter):
        def filter(self, record):
            return 'Kline for' in record.getMessage() or 'Real-time candle buffer' in record.getMessage()
    candle_file_handler.addFilter(KlineOnlyFilter())

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)  # Show DEBUG in console for troubleshooting
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    console_handler.addFilter(CandleFilter())

    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(candle_file_handler)
        logger.addHandler(console_handler)

    return logger

def log_trade(trade):
    try:
        logger = logging.getLogger('TradingBot')
        entry_balance = trade.get('entry_balance', 0)
        exit_balance = trade.get('exit_balance', float('nan'))
        profit_inr = trade.get('profit_inr', float('nan'))
        timestamp = trade.get('exit_time', datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
        log_msg = (
            f"[TRADE LOG: {timestamp}] Symbol: {trade['symbol']} | Timeframe: {trade['timeframe']} | "
            f"Side: {trade['signal'].capitalize()} | Entry: {trade['entry_price']:.2f} USDT | "
            f"Exit: {trade.get('exit_price', 'N/A')} USDT | Profit: {profit_inr:.2f} INR | "
            f"Entry Balance: {entry_balance:.2f} USDT | Exit Balance: {exit_balance:.2f} USDT | "
            f"Leverage: {trade['leverage']}x | Strategy: {trade['strategy']}"
        )
        logger.info(log_msg)
    except Exception as e:
        logger.error(f"Error logging trade: {e}")