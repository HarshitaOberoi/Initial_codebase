import asyncio
import websockets
import json
import time
import hmac
import hashlib
import pandas as pd
import random
import yaml
from src.utils.logger import setup_logger
from src.exchange.coindcx_client import to_api_symbol, to_config_symbol

logger = setup_logger()

class WebSocketClient:
    def __init__(self, config_path="config/config.yaml", fetcher=None, strategy_manager=None, executor=None):
        logger.debug("Initializing WebSocketClient")
        if isinstance(config_path, dict):
            self.config = config_path
        else:
            try:
                with open(config_path, 'r') as f:
                    self.config = yaml.safe_load(f)
            except Exception as error:
                logger.error(f"Failed to load config: {error}")
                raise
        self.simulated = self.config.get('trading', {}).get('simulated', False)
        self.symbols = [self.config.get('trading', {}).get('symbol', 'BTCUSDT')] + \
                       self.config.get('trading', {}).get('additional_symbols', [])
        self.timeframes = self.config.get('trading', {}).get('timeframes', ['1m'])
        self.exchange_rate = self.config.get('trading', {}).get('exchange_rate_usdt_inr', 86)
        self.websocket_url = self.config.get('websocket', {}).get('url', 'wss://stream.coindcx.com')
        self.api_key = self.config.get('api', {}).get('coindcx', {}).get('api_key', '')
        self.api_secret = self.config.get('api', {}).get('coindcx', {}).get('api_secret', '')
        self.min_rows = self.config.get('websocket', {}).get('min_rows', 20)
        required_fields = ['trading.symbol', 'trading.timeframes', 'websocket.url']
        for field in required_fields:
            if not self.config.get(field.split('.')[0], {}).get(field.split('.')[1]):
                raise ValueError(f"Missing required config field: {field}")
        if not self.simulated and (not self.api_key or not self.api_secret):
            raise ValueError("API key and secret are required for live trading")
        self.fetcher = fetcher
        self.strategy_manager = strategy_manager
        self.executor = executor
        self.running = False
        self.trade_buffer = {}
        self.candle_buffer = {}
        self.order_book = {s: {'bids': [], 'asks': [], 'timestamp': 0} for s in self.symbols}
        self.connected = False
        self.last_activity_time = time.time()
        self.max_retries = 10
        self.connection_lock = asyncio.Lock()
        self.channels = ['coindcx'] if not self.simulated else []
        self.timeframe_map = {
            '1m': '1', '5m': '5', '15m': '15', '30m': '30', '1h': '60',
            '2h': '120', '4h': '240', '6h': '360', '12h': '720',
            '1d': '1D', '3d': '3D', '1w': '1W'
        }
        for symbol in self.symbols:
            api_symbol = to_api_symbol(symbol, is_futures=True)
            if self.fetcher and api_symbol not in self.fetcher.valid_pairs:
                logger.warning(f"Skipping invalid symbol {symbol} ({api_symbol})")
                continue
            self.channels.append(f"{api_symbol}@orderbook@10-futures")
            for tf in self.timeframes:
                resolution = self.timeframe_map.get(tf, '1')
                self.channels.append(f"{api_symbol}_{resolution}-futures")
        logger.info(f"WebSocketClient initialized with channels: {self.channels}")
        logger.info(f"Running in {'simulated' if self.simulated else 'live'} mode")

    async def start(self):
        self.running = True
        logger.debug("Starting WebSocketClient")
        try:
            if self.fetcher:
                for symbol in self.symbols:
                    for tf in self.timeframes:
                        try:
                            data = await self.fetcher.fetch_data(symbol, tf, limit=self.min_rows)
                            buffer_key = f"{symbol}_{tf}"
                            self.candle_buffer[buffer_key] = data
                            logger.info(f"Loaded {len(data)} initial rows for {symbol} ({tf})")
                            if len(data) >= self.min_rows and self.strategy_manager:
                                result = await self.strategy_manager.process_data(
                                    symbol, tf, data.copy(), order_book=self.order_book.get(symbol, {'bids': [], 'asks': []})
                                )
                                logger.debug(f"Initial StrategyManager result for {symbol} ({tf}): {result}")
                        except Exception as error:
                            logger.error(f"Failed to load initial data for {symbol} ({tf}): {error}")
                            self.candle_buffer[buffer_key] = pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    try:
                        order_book = await self.fetcher.fetch_order_book(symbol)
                        self.order_book[symbol] = {
                            'bids': order_book.get('bids', []),
                            'asks': order_book.get('asks', []),
                            'timestamp': int(time.time() * 1000)
                        }
                        logger.info(f"Initialized order book for {symbol}: {len(self.order_book[symbol]['bids'])} bids, {len(self.order_book[symbol]['asks'])} asks")
                    except Exception as error:
                        logger.error(f"Failed to initialize order book for {symbol}: {error}")
            asyncio.create_task(self.cleanup_buffers())
            asyncio.create_task(self.log_stats())
            asyncio.create_task(self.order_book_refresh_task())
            if self.simulated:
                logger.debug("Starting simulated data loop")
                await self.simulated_data_loop()
            else:
                logger.debug("Starting WebSocket loop")
                await self.websocket_loop()
        except Exception as error:
            logger.error(f"WebSocketClient critical error: {error}", exc_info=True)
            await self.stop()

    async def websocket_loop(self):
        retry_count = 0
        base_delay = 5
        while self.running and retry_count < self.max_retries:
            async with self.connection_lock:
                if self.connected:
                    logger.debug("Already connected, skipping reconnect")
                    await asyncio.sleep(5)
                    continue
                try:
                    logger.info(f"Connecting to WebSocket: {self.websocket_url} (Attempt {retry_count + 1}/{self.max_retries})")
                    async with websockets.connect(self.websocket_url, ping_interval=20, ping_timeout=10) as ws:
                        self.connected = True
                        logger.info(f"Connected to WebSocket server at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                        await self.subscribe_channels(ws)
                        asyncio.create_task(self.heartbeat_task(ws))
                        while self.running:
                            try:
                                message = await ws.recv()
                                self.last_activity_time = time.time()
                                await self.on_message(json.loads(message))
                            except websockets.ConnectionClosed:
                                logger.warning("WebSocket connection closed, attempting to reconnect")
                                self.connected = False
                                break
                except Exception as error:
                    retry_count += 1
                    logger.error(f"Connection failed: {error}", exc_info=True)
                    self.connected = False
                    if retry_count >= self.max_retries:
                        logger.error("Max retries reached, switching to fallback mode")
                        await self.fallback_mode()
                        break
                    if self.running:
                        reconnect_delay = min(base_delay * 2 ** (retry_count - 1) + random.uniform(0, 0.5), 60)
                        logger.info(f"Retrying in {reconnect_delay:.2f} seconds...")
                        await asyncio.sleep(reconnect_delay)

    async def subscribe_channels(self, ws):
        try:
            for channel in self.channels:
                await ws.send(json.dumps({
                    'method': 'join',
                    'params': {'channelName': channel}
                }))
                if channel == 'coindcx':
                    body = {"channelName": "coindcx", "timestamp": int(time.time() * 1000)}
                    json_body = json.dumps(body, separators=(',', ':'))
                    signature = hmac.new(
                        bytes(self.api_secret, 'utf-8'),
                        json_body.encode('utf-8'),
                        hashlib.sha256
                    ).hexdigest()
                    await ws.send(json.dumps({
                        'method': 'join',
                        'params': {
                            'channelName': 'coindcx',
                            'authSignature': signature,
                            'apiKey': self.api_key
                        }
                    }))
                logger.info(f"Subscribed to {channel}")
                await asyncio.sleep(0.2)
        except Exception as error:
            logger.error(f"Error subscribing to channels: {error}")
            self.connected = False

    async def heartbeat_task(self, ws):
        while self.running and self.connected:
            try:
                await ws.ping()
                logger.debug("Sent ping")
                await asyncio.sleep(25)  # Ping every 25 seconds per docs
                if time.time() - self.last_activity_time > 60:
                    logger.warning("No server activity received, reconnecting")
                    self.connected = False
                    break
            except Exception as error:
                logger.error(f"Error in heartbeat task: {error}")
                self.connected = False
                break

    async def fallback_mode(self):
        logger.info("Entering fallback mode: Using DataFetcher for periodic updates")
        while self.running:
            try:
                for symbol in self.symbols:
                    for tf in self.timeframes:
                        data = await self.fetcher.fetch_data(symbol, tf, limit=1)
                        if not data.empty:
                            await self.on_candlestick({
                                'method': 'candlestick',
                                'params': {
                                    'pair': to_api_symbol(symbol, is_futures=True),
                                    'i': self.timeframe_map.get(tf, '1'),
                                    'data': [{
                                        'open_time': int(data.iloc[-1]['timestamp'].timestamp()),
                                        'open': data.iloc[-1]['open'],
                                        'high': data.iloc[-1]['high'],
                                        'low': data.iloc[-1]['low'],
                                        'close': data.iloc[-1]['close'],
                                        'volume': data.iloc[-1]['volume']
                                    }]
                                }
                            })
                    try:
                        order_book = await self.fetcher.fetch_order_book(symbol)
                        self.order_book[symbol] = {
                            'bids': order_book.get('bids', []),
                            'asks': order_book.get('asks', []),
                            'timestamp': int(time.time() * 1000)
                        }
                        if hasattr(self.executor, 'order_book'):
                            self.executor.order_book[symbol] = self.order_book[symbol]
                        logger.info(f"Fallback: Updated order book for {symbol}: {len(self.order_book[symbol]['bids'])} bids, {len(self.order_book[symbol]['asks'])} asks")
                    except Exception as error:
                        logger.error(f"Fallback: Failed to update order book for {symbol}: {error}")
                await asyncio.sleep(60)
            except Exception as error:
                logger.error(f"Error in fallback mode: {error}")
                await asyncio.sleep(60)

    async def simulated_data_loop(self):
        while self.running:
            try:
                for symbol in self.symbols:
                    base_price = 108510 if symbol == 'BTCUSDT' else 2427 if symbol == 'ETHUSDT' else 150
                    price = base_price + (random.random() - 0.5) * base_price * 0.02
                    precision = self.fetcher.get_precision(symbol)['price_precision'] if self.fetcher else 2
                    price = round(price, precision)
                    data = {
                        'method': 'trade',
                        'params': {
                            'pair': symbol,
                            'p': price,
                            'q': random.uniform(0.1, 10),
                            'T': int(time.time())
                        }
                    }
                    await self.on_new_trade(data)
                    self.order_book[symbol] = {
                        'bids': [(round(price - i * 0.1, precision), random.uniform(1, 10)) for i in range(10)],
                        'asks': [(round(price + i * 0.1, precision), random.uniform(1, 10)) for i in range(10)],
                        'timestamp': int(time.time() * 1000)
                    }
                    if hasattr(self.executor, 'order_book'):
                        self.executor.order_book[symbol] = self.order_book[symbol]
                    for tf in self.timeframes:
                        kline_data = {
                            'method': 'candlestick',
                            'params': {
                                'pair': symbol,
                                'i': self.timeframe_map.get(tf, '1'),
                                'data': [{
                                    'open_time': int(time.time()),
                                    'open': price,
                                    'high': round(price + random.uniform(0, base_price * 0.005), precision),
                                    'low': round(price - random.uniform(0, base_price * 0.005), precision),
                                    'close': round(price + random.uniform(-base_price * 0.002, base_price * 0.002), precision),
                                    'volume': random.uniform(10, 100)
                                }]
                            }
                        }
                        await self.on_candlestick(kline_data)
                    logger.debug(f"Generated simulated data for {symbol}")
                    await asyncio.sleep(1 / len(self.symbols))
            except Exception as error:
                logger.error(f"Error in simulated data loop: {error}")
                await asyncio.sleep(1)

    async def cleanup_buffers(self):
        while self.running:
            try:
                current_time = time.time() * 1000
                for key in list(self.candle_buffer.keys()):
                    if key not in [f"{s}_{tf}" for s in self.symbols for tf in self.timeframes]:
                        del self.candle_buffer[key]
                        logger.debug(f"Cleaned stale candle buffer: {key}")
                    elif not self.candle_buffer[key].empty:
                        self.candle_buffer[key] = self.candle_buffer[key][
                            self.candle_buffer[key]['timestamp'].apply(lambda x: (current_time - x.timestamp() * 1000) < 24 * 3600 * 1000)
                        ].tail(self.min_rows)
                for key in list(self.trade_buffer.keys()):
                    if key not in [f"{s}_{tf}" for s in self.symbols for tf in self.timeframes]:
                        del self.trade_buffer[key]
                        logger.debug(f"Cleaned stale trade buffer: {key}")
                    elif not self.trade_buffer[key].empty:
                        self.trade_buffer[key] = self.trade_buffer[key][
                            self.trade_buffer[key]['timestamp'].apply(lambda x: (current_time - x.timestamp() * 1000) < 24 * 3600 * 1000)
                        ].tail(self.min_rows)
                await asyncio.sleep(3600)
            except Exception as error:
                logger.error(f"Error cleaning buffers: {error}")
                await asyncio.sleep(3600)

    async def log_stats(self):
        while self.running:
            try:
                logger.info(f"Buffer sizes: candle={len(self.candle_buffer)}, trade={len(self.trade_buffer)}")
                order_book_sizes = {s: len(self.order_book[s]['bids']) for s in self.order_book}
                logger.info(f"Order book sizes: {order_book_sizes}")
                logger.info(f"WebSocket connected: {self.connected}")
                for symbol in self.symbols:
                    if self.order_book[symbol]['timestamp'] < time.time() * 1000 - 300000:
                        logger.warning(f"Order book for {symbol} is stale (last updated {int(time.time() - self.order_book[symbol]['timestamp']/1000)}s ago)")
            except Exception as error:
                logger.error(f"Error logging stats: {error}")
                await asyncio.sleep(3600)

    async def order_book_refresh_task(self):
        while self.running:
            try:
                for symbol in self.symbols:
                    if not self.connected or self.order_book[symbol]['timestamp'] < time.time() * 1000 - 60000:
                        try:
                            order_book = await self.fetcher.fetch_order_book(symbol)
                            self.order_book[symbol] = {
                                'bids': order_book.get('bids', []),
                                'asks': order_book.get('asks', []),
                                'timestamp': int(time.time() * 1000)
                            }
                            if hasattr(self.executor, 'order_book'):
                                self.executor.order_book[symbol] = self.order_book[symbol]
                            logger.info(f"Refreshed order book for {symbol}: {len(self.order_book[symbol]['bids'])} bids, {len(self.order_book[symbol]['asks'])} asks")
                        except Exception as error:
                            logger.error(f"Failed to refresh order book for {symbol}: {error}")
                await asyncio.sleep(60)
            except Exception as error:
                logger.error(f"Error in order book refresh task: {error}")
                await asyncio.sleep(60)

    async def on_message(self, message):
        try:
            logger.debug(f"WebSocket message received: {message}")
            method = message.get('method')
            params = message.get('params', {})
            if method == 'depth-snapshot':
                await self.on_depth_snapshot({'data': params})
            elif method == 'depth-update':
                await self.on_depth_update({'data': params})
            elif method == 'candlestick':
                await self.on_candlestick({'method': 'candlestick', 'params': params})
            elif method == 'trade':
                await self.on_new_trade({'data': params})
            elif method == 'df-position-update':
                await self.on_position_update({'data': params})
            elif method == 'df-order-update':
                await self.on_order_update({'data': params})
            elif method == 'balance-update':
                await self.on_balance_update({'data': params})
            else:
                logger.debug(f"Unhandled event {method}: {message}")
        except Exception as error:
            logger.error(f"Error processing message: {error}", exc_info=True)

    async def on_candlestick(self, data):
        try:
            self.last_activity_time = time.time()
            logger.debug(f"Raw kline data: {data}")
            if not isinstance(data, dict) or 'params' not in data or not data['params'].get('data'):
                logger.error(f"Invalid kline data structure: {data}")
                return
            kline_data = data['params']['data'][0]
            symbol = to_config_symbol(kline_data.get('pair', ''))
            api_symbol = to_api_symbol(symbol, is_futures=True)
            if api_symbol is None or '__' in api_symbol or api_symbol not in self.fetcher.valid_pairs:
                logger.error(f"Invalid symbol {symbol} (API: {api_symbol}), Valid pairs: {list(self.fetcher.valid_pairs)[:10]}")
                return
            if symbol not in self.symbols:
                logger.error(f"Invalid symbol {symbol} in kline data")
                return
            if symbol.endswith('INR'):
                logger.debug(f"Skipping INR pair {symbol}")
                return
            timeframe = data['params'].get('i', '')
            if timeframe not in self.timeframe_map.values():
                logger.error(f"Invalid timeframe {timeframe} for {symbol}")
                return
            timeframe = [k for k, v in self.timeframe_map.items() if v == timeframe][0]
            close = float(kline_data.get('close', 0))
            open_time = kline_data.get('open_time', int(time.time()))
            timestamp = pd.to_datetime(open_time, unit='s', errors='coerce') or pd.Timestamp.now()
            if abs((timestamp.timestamp() * 1000) - time.time() * 1000) > 60000:
                logger.warning(f"Timestamp misalignment for {symbol} ({timeframe}): {timestamp}")
                timestamp = pd.Timestamp.now()
            if close <= 0:
                logger.error(f"Invalid kline for {symbol}: close={close}")
                return
            df = pd.DataFrame([{
                'timestamp': timestamp,
                'open': float(kline_data.get('open', close)),
                'high': float(kline_data.get('high', close)),
                'low': float(kline_data.get('low', close)),
                'close': close,
                'volume': float(kline_data.get('volume', 0))
            }])
            logger.info(f"Kline for {symbol} ({timeframe}):\n{df.to_string(index=False)}")
            buffer_key = f"{symbol}_{timeframe}"
            if buffer_key not in self.candle_buffer:
                self.candle_buffer[buffer_key] = pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            self.candle_buffer[buffer_key] = pd.concat([self.candle_buffer[buffer_key], df], ignore_index=True).tail(self.min_rows)
            logger.debug(f"Candle buffer for {buffer_key}: Rows={len(self.candle_buffer[buffer_key])}, NaNs={self.candle_buffer[buffer_key].isna().sum().to_dict()}")
            if len(self.candle_buffer[buffer_key]) >= self.min_rows and self.strategy_manager:
                result = await self.strategy_manager.process_data(
                    symbol, timeframe, self.candle_buffer[buffer_key].copy(),
                    order_book=self.order_book.get(symbol, {'bids': [], 'asks': [], 'timestamp': 0})
                )
                if result and isinstance(result, tuple) and len(result) == 2:
                    signal, params = result
                    logger.debug(f"StrategyManager result for {symbol} ({timeframe}): Signal={signal}, Params={params}")
                    if signal != 'any' and signal in ['buy', 'sell']:
                        logger.info(f"Executing trade signal for {api_symbol} ({timeframe}): {signal}, Params={params}")
                        await self.executor.execute_trade(api_symbol, timeframe, signal, params, params.get('strategy', 'unknown'))
                        logger.info(f"Processed signal {signal} for {api_symbol} ({timeframe})")
                    logger.debug(f"Sent {len(self.candle_buffer[buffer_key])} rows for {symbol} ({timeframe}) to StrategyManager")
        except Exception as error:
            logger.error(f"Error processing kline: {error}", exc_info=True)
            buffer_key = f"{symbol}_{timeframe}"
            if buffer_key in self.candle_buffer:
                self.candle_buffer[buffer_key] = pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    async def on_new_trade(self, data):
        try:
            self.last_activity_time = time.time()
            logger.debug(f"Raw trade data: {data}")
            if not isinstance(data, dict) or 'data' not in data or not data['data']:
                logger.error(f"Invalid trade data: {data}")
                return
            trade_data = data['data']
            symbol = to_config_symbol(trade_data.get('pair', ''))
            if symbol not in self.symbols:
                logger.error(f"Invalid symbol {symbol} in trade data")
                return
            if symbol.endswith('INR'):
                logger.debug(f"Skipping INR pair {symbol}")
                return
            price = float(trade_data.get('p', 0))
            volume = float(trade_data.get('q', 0))
            timestamp = pd.to_datetime(trade_data.get('T', int(time.time())), unit='s', errors='coerce') or pd.Timestamp.now()
            if abs((timestamp.timestamp() * 1000) - time.time() * 1000) > 60000:
                logger.warning(f"Timestamp misalignment for {symbol} trade: {timestamp}")
                timestamp = pd.Timestamp.now()
            if price <= 0 or volume <= 0:
                logger.error(f"Invalid trade for {symbol}: price={price}, volume={volume}")
                return
            df = pd.DataFrame([{
                'timestamp': timestamp,
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'volume': volume
            }])
            price_inr = price * self.exchange_rate
            logger.info(f"Trade for {symbol}: Price={price:.2f} USDT ({price_inr:.2f} INR), Volume={volume:.2f}")
            for timeframe in self.timeframes:
                buffer_key = f"{symbol}_{timeframe}"
                if buffer_key not in self.trade_buffer:
                    self.trade_buffer[buffer_key] = pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                self.trade_buffer[buffer_key] = pd.concat([self.trade_buffer[buffer_key], df], ignore_index=True)
                resampled_df = self.trade_buffer[buffer_key].set_index('timestamp').resample(timeframe).agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                }).fillna(method='ffill').reset_index().tail(self.min_rows)
                self.trade_buffer[buffer_key] = resampled_df
                logger.debug(f"Trade buffer for {buffer_key}: Rows={len(self.trade_buffer[buffer_key])}, NaNs={self.trade_buffer[buffer_key].isna().sum().to_dict()}")
                if len(self.trade_buffer[buffer_key]) >= self.min_rows and self.strategy_manager:
                    result = await self.strategy_manager.process_data(
                        symbol, timeframe, self.trade_buffer[buffer_key].copy(),
                        order_book=self.order_book.get(symbol, {'bids': [], 'asks': [], 'timestamp': 0})
                    )
                    if result and isinstance(result, tuple) and len(result) == 2:
                        signal, params = result
                        logger.debug(f"StrategyManager result for {symbol} ({timeframe}): Signal={signal}, Params={params}")
                        if signal != 'any' and signal in ['buy', 'sell']:
                            api_symbol = to_api_symbol(symbol, is_futures=True)
                            logger.info(f"Executing trade signal for {api_symbol} ({timeframe}): {signal}, Params={params}")
                            await self.executor.execute_trade(api_symbol, timeframe, signal, params, params.get('strategy', 'unknown'))
                            logger.info(f"Processed signal {signal} for {api_symbol} ({timeframe})")
                    logger.debug(f"Sent {len(self.trade_buffer[buffer_key])} rows for {symbol} ({timeframe}) to StrategyManager")
        except Exception as error:
            logger.error(f"Error processing trade: {error}", exc_info=True)
            for timeframe in self.timeframes:
                buffer_key = f"{symbol}_{timeframe}"
                if buffer_key in self.trade_buffer:
                    self.trade_buffer[buffer_key] = pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    async def on_depth_snapshot(self, data):
        try:
            self.last_activity_time = time.time()
            logger.debug(f"Raw depth snapshot: {data}")
            if not isinstance(data, dict) or 'data' not in data:
                logger.error(f"Invalid depth snapshot: {data}")
                return
            depth_data = data['data']
            symbol = to_config_symbol(depth_data.get('pair', ''))
            if symbol not in self.symbols:
                logger.error(f"Invalid symbol {symbol} in depth snapshot")
                return
            if symbol.endswith('INR'):
                logger.debug(f"Skipping INR pair {symbol}")
                return
            self.order_book[symbol] = {
                'bids': [(float(p), float(q)) for p, q in depth_data.get('bids', {}).items() if float(p) > 0 and float(q) > 0],
                'asks': [(float(p), float(q)) for p, q in depth_data.get('asks', {}).items() if float(p) > 0 and float(q) > 0],
                'timestamp': depth_data.get('ts', int(time.time() * 1000))
            }
            logger.info(f"Depth snapshot updated for {symbol}: {len(self.order_book[symbol]['bids'])} bids, {len(self.order_book[symbol]['asks'])} asks")
            if hasattr(self.executor, 'order_book'):
                self.executor.order_book[symbol] = self.order_book[symbol]
        except Exception as error:
            logger.error(f"Error processing depth snapshot: {error}", exc_info=True)
            if symbol in self.order_book:
                self.order_book[symbol] = {'bids': [], 'asks': [], 'timestamp': 0}

    async def on_depth_update(self, data):
        try:
            self.last_activity_time = time.time()
            logger.debug(f"Raw depth-update data: {data}")
            if not isinstance(data, dict) or 'data' not in data:
                logger.error(f"Invalid depth data: {data}")
                return
            depth_data = data['data']
            symbol = to_config_symbol(depth_data.get('pair', ''))
            if symbol not in self.order_book:
                logger.error(f"Invalid symbol {symbol} in depth update")
                return
            if symbol.endswith('INR'):
                logger.debug(f"Skipping INR pair {symbol}")
                return
            bids = [(float(p), float(q)) for p, q in depth_data.get('bids', {}).items() if float(p) > 0 and float(q) >= 0]
            asks = [(float(p), float(q)) for p, q in depth_data.get('asks', {}).items() if float(p) > 0 and float(q) >= 0]
            current_book = self.order_book[symbol]
            for price, qty in bids:
                current_book['bids'] = [(p, q) for p, q in current_book['bids'] if p != price]
                if qty > 0:
                    current_book['bids'].append((price, qty))
            for price, qty in asks:
                current_book['asks'] = [(p, q) for p, q in current_book['asks'] if p != price]
                if qty > 0:
                    current_book['asks'].append((price, qty))
            current_book['bids'] = sorted(current_book['bids'], reverse=True)[:20]
            current_book['asks'] = sorted(current_book['asks'])[:20]
            current_book['timestamp'] = depth_data.get('ts', int(time.time() * 1000))
            if hasattr(self.executor, 'order_book'):
                self.executor.order_book[symbol] = current_book
            bid_inr = current_book['bids'][0][0] * self.exchange_rate if current_book['bids'] else 0
            ask_inr = current_book['asks'][0][0] * self.exchange_rate if current_book['asks'] else 0
            logger.info(f"Order book: {symbol} bid={bid_inr:.2f} INR, ask={ask_inr:.2f} INR")
        except Exception as error:
            logger.error(f"Error processing depth-update: {error}", exc_info=True)
            if symbol in self.order_book:
                self.order_book[symbol] = {'bids': [], 'asks': [], 'timestamp': 0}

    async def on_position_update(self, data):
        try:
            self.last_activity_time = time.time()
            logger.debug(f"Raw position update: {data}")
            if not isinstance(data, dict) or 'data' not in data:
                logger.error(f"Invalid position update: {data}")
                return
            position_data = data['data']
            symbol = to_config_symbol(position_data.get('s', ''))
            if symbol not in self.symbols:
                logger.error(f"Invalid symbol {symbol} in position update")
                return
            if symbol.endswith('INR'):
                logger.debug(f"Skipping INR pair {symbol}")
                return
            qty = float(position_data.get('q', 0))
            price = float(position_data.get('p', 0))
            if qty != 0:
                logger.info(f"Position update for {symbol}: Quantity={qty}, EntryPrice={price:.2f}")
                if self.executor:
                    await self.executor.update_position(symbol, qty, price)
        except Exception as error:
            logger.error(f"Error processing position update: {error}", exc_info=True)

    async def on_order_update(self, data):
        try:
            self.last_activity_time = time.time()
            logger.debug(f"Raw order update: {data}")
            if not isinstance(data, dict) or 'data' not in data:
                logger.error(f"Invalid order update: {data}")
                return
            order_data = data['data']
            symbol = to_config_symbol(order_data.get('s', ''))
            if symbol not in self.symbols:
                logger.error(f"Invalid symbol {symbol} in order update")
                return
            if symbol.endswith('INR'):
                logger.debug(f"Skipping INR order {symbol}")
                return
            order_id = order_data.get('id', '')
            status = order_data.get('status', '')
            if order_id and status:
                logger.info(f"Order update for {symbol}: OrderID={order_id}, Status={status}")
                if self.executor:
                    await self.executor.update_order(symbol, order_id, status, order_data)
        except Exception as error:
            logger.error(f"Error processing order update: {error}", exc_info=True)

    async def on_balance_update(self, data):
        try:
            self.last_activity_time = time.time()
            logger.debug(f"Raw balance update: {data}")
            if not isinstance(data, dict) or 'data' not in data:
                logger.error(f"Invalid balance data: {data}")
                return
            balance_data = data['data']
            balance = float(balance_data.get('balance', 0))
            currency = balance_data.get('currency', '')
            logger.info(f"Balance updated: {currency}={balance:.2f}")
            if self.executor:
                await self.executor.update_balance(currency, balance)
        except Exception as error:
            logger.error(f"Error processing balance update: {error}", exc_info=True)

    async def stop(self):
        self.running = False
        self.connected = False
        if self.executor:
            try:
                await self.executor.close_all_trades()
            except Exception as error:
                logger.error(f"Error closing trades: {error}")
        logger.info("WebSocketClient stopped")