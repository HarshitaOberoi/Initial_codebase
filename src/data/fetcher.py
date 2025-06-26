
import pandas as pd
import asyncio
import yaml
import random
import numpy as np
from datetime import datetime
from src.utils.logger import setup_logger
from src.exchange.coindcx_client import CoinDCXClient, to_api_symbol
import requests

logger = setup_logger()

class DataFetcher:
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
        self.client = CoinDCXClient(config=self.config)
        self.base_url = 'https://public.coindcx.com'
        self.market_precision = {}
        self.valid_pairs = set()
        self.timeframe_map = {
            '1m': '1', '5m': '5', '15m': '15', '30m': '30', '1h': '60',
            '2h': '120', '4h': '240', '6h': '360', '12h': '720',
            '1d': '1D', '3d': '3D', '1w': '1W'
        }
        self.simulated = self.client.simulated
        asyncio.create_task(self._load_market_pairs())  # Initialize valid pairs

    async def _fetch_historical_data(self, symbol, interval, limit=100):
        api_symbol = to_api_symbol(symbol, is_futures=True)
        if api_symbol is None or '__' in api_symbol:
            logger.error(f"Invalid symbol {symbol} (API: {api_symbol})")
            return pd.DataFrame()
        if api_symbol not in self.valid_pairs:
            logger.info(f"Symbol {api_symbol} not in valid pairs, refreshing...")
            await self._load_market_pairs()
            if api_symbol not in self.valid_pairs:
                logger.warning(f"Invalid or unsupported pair: {api_symbol}. Available pairs: {list(self.valid_pairs)[:10]}")
                return pd.DataFrame()
        if self.simulated:
            base_price = 108510 if 'BTC' in symbol else 2427 if 'ETH' in symbol else 150
            precision = self.client.price_precision.get(api_symbol, 2)
            data = []
            for i in range(limit):
                price = base_price + (random.random() - 0.5) * base_price * 0.02
                data.append({
                    'time': (datetime.now() - pd.Timedelta(minutes=i)).timestamp() * 1000,
                    'open': round(price, precision),
                    'high': round(price + random.uniform(0, base_price * 0.005), precision),
                    'low': round(price - random.uniform(0, base_price * 0.005), precision),
                    'close': round(price + random.uniform(-base_price * 0.002, base_price * 0.002), precision),
                    'volume': random.uniform(10, 100)
                })
            df = pd.DataFrame(data, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['time'], unit='ms')
            df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
            df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
            logger.info(f"Generated {len(df)} candles for {symbol} ({interval})")
            logger.info(f"Sample data for {symbol} ({interval}):\n{df.head(3).to_string(index=False)}")
            return df.sort_values('timestamp', ascending=True)
        resolution = self.timeframe_map.get(interval, '1')
        end_time = int(datetime.now().timestamp())
        seconds_per_candle = self._interval_to_seconds(resolution)
        start_time = end_time - (limit * seconds_per_candle * 5)
        endpoint = "/market_data/candlesticks"
        params = {
            'pair': api_symbol,
            'from': start_time,
            'to': end_time,
            'resolution': resolution,
            'pcode': 'f'
        }
        try:
            logger.debug(f"Fetching candlestick data for {symbol} ({interval}): {params}")
            data = await self.client._async_request('GET', endpoint, params=params, use_public_api=True)
            logger.debug(f"Data received for {symbol} ({interval}): {data}")
            if data and isinstance(data, dict) and data.get('s') == 'ok' and isinstance(data.get('data'), list):
                df = pd.DataFrame(data['data'], columns=['time', 'open', 'high', 'low', 'volume', 'close'])
                df['timestamp'] = pd.to_datetime(df['time'], unit='ms')
                df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
                df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
                logger.info(f"Fetched {len(df)} candles for {symbol} ({interval})")
                logger.info(f"Sample data for {symbol} ({interval}):\n{df.head(3).to_string(index=False)}")
                return df.sort_values('timestamp', ascending=True)
            else:
                logger.warning(f"No valid candlestick data returned for {symbol} ({interval}): {data}")
                if resolution == '1':
                    logger.info(f"Retrying with 5m resolution for {symbol}")
                    return await self._fetch_historical_data(symbol, '5m', limit)
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error fetching candlestick data for {symbol} ({interval}): {e}", exc_info=True)
            return pd.DataFrame()

    def _interval_to_seconds(self, interval):
        if interval == '1W':
            return 7 * 24 * 60 * 60
        elif interval == '3D':
            return 3 * 24 * 60 * 60
        elif interval == '1D':
            return 24 * 60 * 60
        elif interval in {'1', '5', '15', '30', '60', '120', '240', '360', '720'}:
            return int(interval) * 60
        return 60

    async def fetch_data(self, symbol, timeframe, limit=100):
        try:
            logger.debug(f"Fetching data for {symbol} ({timeframe})")
            df = await self._fetch_historical_data(symbol, timeframe, limit=100)  # Increased limit
            logger.info(f"Loaded {len(df)} initial rows for {symbol} ({timeframe})")
            logger.debug(f"Returning DataFrame for {symbol} ({timeframe}): {len(df)} rows, empty={df.empty}")
            return df
        except Exception as e:
            logger.error(f"Error fetching data for {symbol} ({timeframe}): {e}")
            return pd.DataFrame()

    async def fetch_order_book(self, symbol, limit=10):
        try:
            api_symbol = to_api_symbol(symbol, is_futures=True)  # e.g., B-ADA_USDT
            endpoint = f"{self.base_url}/market_data/v3/orderbook/{api_symbol}-futures/{limit}"
            response = requests.get(endpoint)
            logger.debug(f"Order book response for {api_symbol}: {response.status_code}, {response.text}")
            response.raise_for_status()
            data = response.json()
            if 'bids' in data and 'asks' in data:
                order_book = {
                    'bids': [[float(price), float(quantity)] for price, quantity in data['bids'].items()],
                    'asks': [[float(price), float(quantity)] for price, quantity in data['asks'].items()],
                    'timestamp': data.get('ts', 0) / 1000
                }
                logger.info(f"Fetched order book for {api_symbol}: {len(order_book['bids'])} bids, {len(order_book['asks'])} asks")
                return order_book
            logger.error(f"Invalid order book data for {api_symbol}: {data}")
            return {'bids': [], 'asks': [], 'timestamp': 0}
        except Exception as e:
            logger.error(f"Error fetching order book for {api_symbol}: {e}")
            return {'bids': [], 'asks': [], 'timestamp': 0}

    async def _load_market_pairs(self):
        try:
            endpoint = "/exchange/v1/derivatives/futures/data/active_instruments"
            response = await self.client._async_request('GET', endpoint, use_public_api=True)
            logger.debug(f"Valid pairs response: {response}")
            if response and isinstance(response, list):
                self.valid_pairs = {pair for pair in response if isinstance(pair, str) and pair.startswith('B-') and pair.endswith('_USDT')}
                if not self.valid_pairs:
                    logger.warning("No USDT futures pairs found, using fallback")
                    self.valid_pairs = {'B-BTC_USDT', 'B-ETH_USDT', 'B-SOL_USDT', 'B-XRP_USDT', 'B-BCH_USDT', 'B-LTC_USDT', 'B-DOGE_USDT', 'B-ADA_USDT'}
            else:
                logger.error(f"Invalid response format: {response}")
                self.valid_pairs = {'B-BTC_USDT', 'B-ETH_USDT', 'B-SOL_USDT', 'B-XRP_USDT', 'B-BCH_USDT', 'B-LTC_USDT', 'B-DOGE_USDT', 'B-ADA_USDT'}
            logger.info(f"Loaded {len(self.valid_pairs)} futures pairs: {list(self.valid_pairs)[:10]}")
            self.market_precision = {
                pair: {'quantity_precision': 8, 'price_precision': 2} for pair in self.valid_pairs
            }
            tasks = []
            for pair in self.valid_pairs:
                endpoint = f"/exchange/v1/derivatives/futures/data/instrument?pair={pair}"
                tasks.append(self.client._async_request('GET', endpoint, params={'pair': pair}, use_public_api=True))
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for pair, result in zip(self.valid_pairs, results):
                if isinstance(result, Exception):
                    logger.debug(f"Error loading precision for {pair}: {result}")
                    self.market_precision[pair]['quantity_precision'] = 8
                elif result and isinstance(result, dict) and result.get('instrument'):
                    quantity_increment = result['instrument'].get('quantity_increment', 0.00000001)
                    price_increment = result['instrument'].get('price_increment', 0.01)
                    quantity_precision = max(0, -int(np.floor(np.log10(float(quantity_increment)))))
                    price_precision = max(0, -int(np.floor(np.log10(float(price_increment)))))
                    self.market_precision[pair] = {
                        'quantity_precision': quantity_precision,
                        'price_precision': price_precision
                    }
                else:
                    logger.debug(f"No instrument data for {pair}: {result}")
            logger.info(f"Loaded precision for {len(self.market_precision)} markets")
        except Exception as e:
            logger.error(f"Error loading market pairs: {e}", exc_info=True)
            self.valid_pairs = {'B-BTC_USDT', 'B-ETH_USDT', 'B-SOL_USDT', 'B-XRP_USDT', 'B-BCH_USDT', 'B-LTC_USDT', 'B-DOGE_USDT', 'B-ADA_USDT'}
            self.market_precision = {
                pair: {'quantity_precision': 8, 'price_precision': 2}
                for pair in self.valid_pairs
            }
            logger.info(f"Using fallback valid pairs: {self.valid_pairs}")

    def get_precision(self, symbol):
        api_symbol = to_api_symbol(symbol, is_futures=True)
        return self.market_precision.get(api_symbol, {'quantity_precision': 8, 'price_precision': 2})

    async def close(self):
        await self.client.close()
        logger.info("DataFetcher session closed")
