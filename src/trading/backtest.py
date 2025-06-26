import pandas as pd
import yaml
from src.utils.logger import setup_logger
from src.data.indicators import Indicators
from src.strategies.strategy_manager import StrategyManager
from src.trading.executor import TradeExecutor

logger = setup_logger()

class Backtester:
    def __init__(self, config_path):
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        self.strategy_manager = StrategyManager(config_path)
        self.executor = TradeExecutor(config_path)
        self.indicators = Indicators()
        self.symbols = [self.config['trading']['symbol']] + self.config['trading']['additional_symbols']
        self.timeframes = self.config['trading']['timeframes']

    def run_backtest(self, historical_data, symbol, timeframe):
        """Run backtest on historical data for a given symbol and timeframe."""
        logger.info(f"Starting backtest for {symbol} ({timeframe})")
        historical_data = self.indicators.calculate_indicators(historical_data)
        trades = []
        initial_balance = self.executor.simulated_balance

        for i in range(50, len(historical_data)):
            candle = historical_data.iloc[i:i+1]
            historical_subset = historical_data.iloc[:i+1]
            signal, params, strategy = self.strategy_manager.evaluate(candle, historical_subset, symbol, timeframe)
            
            if signal in ['buy', 'sell'] and symbol not in self.executor.active_positions:
                latest_price = candle['close'].iloc[0]
                quantity = self.config['trading']['position_size'] / latest_price
                success = self.executor.place_order(
                    symbol=symbol,
                    side=signal,
                    price=latest_price,
                    quantity=quantity,
                    stop_loss=params['stop_loss'],
                    take_profit=params['take_profit'],
                    strategy=strategy,
                    historical_data=historical_subset
                )
                if success:
                    trades.append({
                        'timestamp': candle['timestamp'].iloc[0],
                        'symbol': symbol,
                        'side': signal,
                        'price': latest_price,
                        'quantity': quantity
                    })
            
            self.executor.manage_positions(candle, symbol, socketio=None, historical_data=historical_subset)

        final_balance = self.executor.simulated_balance
        returns = (final_balance - initial_balance) / initial_balance * 100
        win_rate = len([t for t in self.executor.trade_history if t.get('profit', 0) > 0]) / len(self.executor.trade_history) if self.executor.trade_history else 0
        
        summary = {
            'symbol': symbol,
            'timeframe': timeframe,
            'initial_balance': initial_balance,
            'final_balance': final_balance,
            'returns': returns,
            'win_rate': win_rate,
            'total_trades': len(self.executor.trade_history),
            'trade_history': self.executor.trade_history
        }
        logger.info(f"Backtest summary: {summary}")
        return summary