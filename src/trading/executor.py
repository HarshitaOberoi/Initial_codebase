import asyncio
import yaml
import pandas as pd
import numpy as np
from datetime import datetime
from tabulate import tabulate
from src.utils.logger import setup_logger
from src.exchange.coindcx_client import to_api_symbol

logger = setup_logger()

class TradeExecutor:
    def __init__(self, config_path="config/config.yaml", client=None):
        if isinstance(config_path, dict):
            self.config = config_path
            logger.debug("Loaded config from dictionary")
        else:
            try:
                with open(config_path, 'r') as f:
                    self.config = yaml.safe_load(f)
            except Exception as error:
                logger.error(f"Failed to load config from file: {error}")
                raise
        self.simulated = self.config.get('trading', {}).get('simulated', True)
        self.exchange_rate = self.config.get('trading', {}).get('exchange_rate_usdt_inr', 86.0)
        self.initial_balance_inr = self.config.get('trading', {}).get('initial_balance', 1000.0)
        self.balance_inr = self.initial_balance_inr
        self.balance = self.balance_inr / self.exchange_rate
        self.risk_per_trade = self.config.get('trading', {}).get('risk_per_trade', 0.01)
        self.min_quantity = self.config.get('trading', {}).get('min_quantity', 0.001)
        self.client = client
        self.open_trades = []
        self.closed_trades = []
        self.order_book = {}
        self.trade_id = 0
        self.max_drawdown = self.config.get('trading', {}).get('max_drawdown', 0.2)
        self.daily_loss_limit = self.config.get('trading', {}).get('daily_loss_limit', 0.1)
        self._cleanup_stale_trades()
        logger.info(f"TradeExecutor initialized: Simulated={self.simulated}, Balance={self.balance_inr:.2f} INR ({self.balance:.2f} USDT)")

    def _cleanup_stale_trades(self):
        stale_trades = [trade for trade in self.open_trades if trade.get('status') == 'open']
        for trade in stale_trades:
            if not trade.get('symbol') or not trade.get('timeframe'):
                logger.debug(f"Skipping invalid stale trade: {trade}")
                continue
            logger.info(f"Closing stale trade: {trade.get('symbol')} ({trade.get('timeframe')})")
            trade['status'] = 'closed'
            if trade.get('exit_price') and trade.get('entry_price'):
                profit = (trade['exit_price'] - trade['entry_price']) * trade['quantity'] * trade['leverage']
                trade['profit_inr'] = profit * self.exchange_rate
                self.balance += profit
                self.balance_inr += trade['profit_inr']
                logger.info(f"Stale trade closed with profit: {trade['profit_inr']:.2f} INR")
            self.closed_trades.append(trade)
        self.open_trades = [trade for trade in self.open_trades if trade.get('status') != 'closed']
        logger.info(f"Open trades after cleanup: {len(self.open_trades)}")

    async def execute_trade(self, symbol, timeframe, signal, params, strategy):
        try:
            logger.info(f"Attempting to execute trade for {symbol} ({timeframe}): Signal={signal}, Strategy={strategy}, Params={params}")
            valid_pairs = self.client.valid_pairs if self.client and self.client.valid_pairs else {'B-BTC_USDT', 'B-ETH_USDT', 'B-SOL_USDT', 'B-XRP_USDT', 'B-BCH_USDT', 'B-LTC_USDT', 'B-DOGE_USDT', 'B-ADA_USDT'}
            if '__' in symbol or symbol not in valid_pairs:
                logger.error(f"Invalid symbol {symbol}. Valid pairs: {list(valid_pairs)[:10]}")
                return False
            if any(t['symbol'] == symbol and t['status'] == 'open' for t in self.open_trades):
                logger.debug(f"Skipping trade for {symbol}: Position already open")
                return False
            if not params or not isinstance(params, dict):
                logger.error(f"No valid params for trade: {symbol} ({timeframe})")
                return False
            self.trade_id += 1
            entry_price = params.get('entry_price', 0.0)
            stop_loss = params.get('stop_loss', 0.0)
            take_profit = params.get('take_profit', 0.0)
            leverage = params.get('leverage', 1.0)
            quantity = params.get('quantity', 0.0)
            for param_name, param_value in [
                ('entry_price', entry_price),
                ('stop_loss', stop_loss),
                ('take_profit', take_profit),
                ('leverage', leverage),
                ('quantity', quantity)
            ]:
                if isinstance(param_value, (str, dict)):
                    try:
                        if isinstance(param_value, dict):
                            for key in ['value', 'price', 'stop_price', 'amount']:
                                if key in param_value:
                                    locals()[param_name] = float(param_value[key])
                                    logger.debug(f"Extracted {param_name} from dict with key '{key}': {locals()[param_name]}")
                                    break
                            else:
                                logger.error(f"Invalid {param_name} dict for {symbol}: {param_value}")
                                return False
                        else:
                            locals()[param_name] = float(param_value)
                    except (ValueError, TypeError):
                        logger.error(f"Invalid {param_name} value for {symbol}: {param_value}")
                        return False
                if param_name in ['entry_price', 'stop_loss', 'leverage', 'quantity'] and \
                (not isinstance(locals()[param_name], (int, float)) or locals()[param_name] <= 0):
                    logger.error(f"Invalid {param_name} for {symbol}: {locals()[param_name]}")
                    return False
                if param_name == 'take_profit' and \
                not isinstance(locals()[param_name], (int, float)):
                    logger.error(f"Invalid {param_name} for {symbol}: {locals()[param_name]}")
                    return False
            stop_loss = round(float(stop_loss), 2)
            take_profit = round(float(take_profit), 2)
            if signal == 'buy':
                if stop_loss >= entry_price:
                    logger.error(f"Invalid stop_loss {stop_loss} for {symbol}: Must be below entry_price {entry_price}")
                    return False
                if (entry_price - stop_loss) / entry_price < 0.0005:
                    logger.error(f"Invalid stop_loss {stop_loss} for {symbol}: Too close to entry_price {entry_price}")
                    return False
            elif signal == 'sell':
                if stop_loss <= entry_price:
                    logger.error(f"Invalid stop_loss {stop_loss} for {symbol}: Must be above entry_price {entry_price}")
                    return False
                if (stop_loss - entry_price) / entry_price < 0.0005:
                    logger.error(f"Invalid stop_loss {stop_loss} for {symbol}: Too close to entry_price {entry_price}")
                    return False
            if quantity < self.min_quantity:
                logger.error(f"Quantity {quantity} below minimum {self.min_quantity} for {symbol}")
                return False
            trade = {
                'trade_id': self.trade_id,
                'symbol': symbol,
                'timeframe': timeframe,
                'signal': signal,
                'entry_price': float(entry_price),
                'quantity': float(quantity),
                'leverage': float(leverage),
                'take_profit': take_profit,
                'stop_loss': stop_loss,
                'entry_time': datetime.utcnow(),
                'entry_balance': self.balance,
                'entry_balance_inr': self.balance_inr,
                'status': 'open',
                'strategy': strategy
            }
            if self.simulated:
                success = await self._simulate_trade_execution(trade)
            else:
                success = await self._execute_real_trade(trade)
            if success:
                self.open_trades.append(trade)
                logger.info(f"Trade opened: ID={trade['trade_id']}, Symbol={symbol}, Side={signal}, Qty={quantity:.4f}, Entry={entry_price:.2f}, Strategy={strategy}, Balance={self.balance_inr:.2f} INR")
                asyncio.create_task(self._monitor_trade(trade))
                self._log_trade_summary()
                return True
            logger.error(f"Trade execution failed for {symbol} ({timeframe})")
            return False
        except Exception as e:
            logger.error(f"Error executing trade for {symbol}: {e}", exc_info=True)
            return False

    async def _simulate_trade_execution(self, trade):
        try:
            symbol = trade['symbol']
            if symbol not in self.order_book or not self.order_book[symbol].get('bids') or not self.order_book[symbol].get('asks'):
                logger.warning(f"No order book data for {symbol}, using entry_price as fallback")
                trade['entry_price'] = trade['entry_price']
                return True
            bid = self.order_book[symbol]['bids'][0][0]
            ask = self.order_book[symbol]['asks'][0][0]
            spread = ask - bid
            if spread / bid > 0.01:  # Reject if spread > 1%
                logger.warning(f"Invalid spread for {symbol}: {spread:.2f} ({spread/bid*100:.2f}%)")
                return False
            trade['entry_price'] = ask if trade['signal'] == 'buy' else bid
            return True
        except Exception as error:
            logger.debug(f"Error simulating trade for {trade['symbol']}: {error}")
            return False

    async def _execute_real_trade(self, trade):
        try:
            if not self.client:
                logger.error("No client defined for real trade execution")
                return False
            symbol = trade['symbol']  # Already in API format (e.g., B-BTC_USDT)
            side = 'buy' if trade['signal'] == 'buy' else 'sell'
            response = await self.client.create_order(symbol, side, trade['quantity'], trade['entry_price'], leverage=trade['leverage'])
            if response is None:
                logger.error(f"Order creation failed for {symbol}: No response")
                return False
            order_id = response.get('order_id')
            if order_id:
                trade['order_id'] = order_id
                logger.info(f"Real trade executed: OrderID={order_id}, Symbol={symbol}, Side={side}")
                return True
            logger.error(f"Order failed for {symbol}: {response}")
            return False
        except Exception as e:
            logger.error(f"Error executing real trade for {trade['symbol']}: {e}", exc_info=True)
            return False

    async def _monitor_trade(self, trade):
        try:
            symbol = trade['symbol']
            while trade['status'] == 'open':
                if symbol in self.order_book and self.order_book[symbol].get('bids') and self.order_book[symbol].get('asks'):
                    current_price = self.order_book[symbol]['bids'][0][0] if trade['signal'] == 'buy' else self.order_book[symbol]['asks'][0][0]
                    if (trade['signal'] == 'buy' and (current_price >= trade['take_profit'] or current_price <= trade['stop_loss'])) or \
                       (trade['signal'] == 'sell' and (current_price <= trade['take_profit'] or current_price >= trade['stop_loss'])):
                        trade['exit_price'] = current_price
                        trade['exit_time'] = datetime.utcnow()
                        trade['exit_balance'] = self.balance
                        trade['exit_balance_inr'] = self.balance_inr
                        profit = (trade['exit_price'] - trade['entry_price']) * trade['quantity'] * trade['leverage'] * (-1 if trade['signal'] == 'sell' else 1)
                        trade['profit_inr'] = profit * self.exchange_rate
                        self.balance += profit
                        self.balance_inr += trade['profit_inr']
                        trade['status'] = 'closed'
                        self.closed_trades.append(trade)
                        self.open_trades = [t for t in self.open_trades if t['trade_id'] != trade['trade_id']]
                        logger.info(f"Trade closed: ID={trade['trade_id']}, Symbol={symbol}, Profit={trade['profit_inr']:.2f} INR, Strategy={trade['strategy']}, Balance={self.balance_inr:.2f} INR")
                        self._log_trade_summary()
                        break
                await asyncio.sleep(1)
        except Exception as error:
            logger.debug(f"Error monitoring trade for {symbol}: {error}")

    def _log_trade_summary(self):
        try:
            trade_data = []
            for trade in self.open_trades + self.closed_trades[-10:]:
                trade_data.append([
                    trade['trade_id'],
                    trade['symbol'],
                    trade['timeframe'],
                    trade['signal'],
                    trade['entry_price'],
                    trade.get('exit_price', 'N/A'),
                    trade['quantity'],
                    trade['leverage'],
                    trade.get('profit_inr', 'N/A'),
                    trade['status'],
                    trade['strategy'],
                    trade['entry_time'].strftime('%Y-%m-%d %H:%M:%S'),
                    trade.get('exit_time', 'N/A') if pd.isna(trade.get('exit_time')) else trade['exit_time'].strftime('%Y-%m-%d %H:%M:%S'),
                    trade['entry_balance_inr'],
                    trade.get('exit_balance_inr', 'N/A')
                ])
            headers = ['ID', 'Symbol', 'TF', 'Side', 'Entry', 'Exit', 'Qty', 'Lev', 'Profit (INR)', 'Status', 'Strategy', 'Entry Time', 'Exit Time', 'Entry Bal (INR)', 'Exit Bal (INR)']
            table = tabulate(trade_data, headers=headers, tablefmt='grid', floatfmt='.2f')
            wins = sum(1 for t in self.closed_trades if t.get('profit_inr', 0) > 0)
            total_trades = len(self.closed_trades)
            win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
            roi = (self.balance_inr - self.initial_balance_inr) / self.initial_balance_inr * 100 if self.initial_balance_inr > 0 else 0
            avg_profit = np.mean([t.get('profit_inr', 0) for t in self.closed_trades if t.get('profit_inr', 0) > 0]) if any(t.get('profit_inr', 0) > 0 for t in self.closed_trades) else 0
            logger.info(f"\nTrade Summary:\n{table}\nBalance: {self.balance_inr:.2f} INR ({self.balance:.2f} USDT)\nWin Rate: {win_rate:.2f}% | ROI: {roi:.2f}% | Avg Profit/Trade: {avg_profit:.2f} INR")
        except Exception as error:
            logger.debug(f"Error logging summary: {error}")

    async def update_position(self, symbol, quantity, price):
        try:
            for trade in self.open_trades:
                if trade['symbol'] == symbol:
                    trade['quantity'] = quantity
                    trade['entry_price'] = price
                    logger.info(f"Updated position for {symbol}: Quantity={quantity}, Price={price:.2f}")
        except Exception as error:
            logger.debug(f"Error updating position for {symbol}: {error}")

    async def update_order(self, symbol, order_id, status, order_data):
        try:
            for trade in self.open_trades:
                if trade.get('order_id') == order_id:
                    trade['status'] = status
                    if status == 'closed':
                        trade['exit_price'] = float(order_data.get('price', trade['entry_price']))
                        trade['exit_time'] = datetime.utcnow()
                        trade['exit_balance'] = self.balance
                        trade['exit_balance_inr'] = self.balance_inr
                        profit = (trade['exit_price'] - trade['entry_price']) * trade['quantity'] * trade['leverage'] * (-1 if trade['signal'] == 'sell' else 1)
                        trade['profit_inr'] = profit * self.exchange_rate
                        self.closed_trades.append(trade)
                        self.open_trades = [t for t in self.open_trades if t['trade_id'] != trade['trade_id']]
                        logger.info(f"Order closed: ID={trade['trade_id']}, Symbol={symbol}, Profit={trade['profit_inr']:.2f} INR, Strategy={trade['strategy']}, Balance={self.balance_inr:.2f} INR")
                        self._log_trade_summary()
        except Exception as error:
            logger.debug(f"Error updating order for {symbol}: {error}")

    async def update_balance(self, currency, balance):
        try:
            if currency == 'INR':
                self.balance_inr = float(balance)
                self.balance = self.balance_inr / self.exchange_rate
                logger.info(f"Balance updated: {self.balance_inr:.2f} INR ({self.balance:.2f} USDT)")
        except Exception as error:
            logger.debug(f"Error updating balance: {error}")

    async def close_all_trades(self):
        try:
            for trade in self.open_trades[:]:
                trade['status'] = 'closed'
                trade['exit_time'] = datetime.utcnow()
                trade['exit_balance'] = self.balance
                trade['exit_balance_inr'] = self.balance_inr
                if trade.get('exit_price'):
                    profit = (trade['exit_price'] - trade['entry_price']) * trade['quantity'] * trade['leverage'] * (-1 if trade['signal'] == 'sell' else 1)
                    trade['profit_inr'] = profit * self.exchange_rate
                    self.balance += profit
                    self.balance_inr += trade['profit_inr']
                    logger.info(f"Closed trade: ID={trade['trade_id']}, Symbol={trade['symbol']}, Profit={trade['profit_inr']:.2f} INR, Strategy={trade['strategy']}, Balance={self.balance_inr:.2f} INR")
                self.closed_trades.append(trade)
                self.open_trades.remove(trade)
            self._log_trade_summary()
        except Exception as error:
            logger.debug(f"Error closing trades: {error}")

    def get_balance(self):
        return self.balance_inr

    def get_open_trades(self):
        return self.open_trades