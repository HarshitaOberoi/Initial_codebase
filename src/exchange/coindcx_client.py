import hmac
import hashlib
import json
import time
import random
import aiohttp
import pandas as pd
import numpy as np
import asyncio
from datetime import datetime
from src.utils.logger import setup_logger

logger = setup_logger()

def to_api_symbol(symbol, is_futures=False):
    try:
        clean_symbol = symbol.replace('B-', '').replace('__', '_').replace('_', '')
        if 'INR' in clean_symbol:
            api_symbol = clean_symbol.replace('INR', '_INR').upper()
        else:
            api_symbol = clean_symbol.replace('USDT', '_USDT').upper()
        if is_futures:
            api_symbol = f"B-{api_symbol}"
        if '__' in api_symbol:
            logger.error(f"Invalid symbol format after conversion: {api_symbol}")
            return None
        return api_symbol
    except Exception as e:
        logger.error(f"Error converting symbol {symbol}: {e}", exc_info=True)
        return None

def to_public_symbol(symbol):
    """
    Convert symbol to public format (e.g., 'BTCUSDT' -> 'BTC-USDT').
    
    Args:
        symbol (str): Original symbol.
    
    Returns:
        str: Public symbol format.
    """
    try:
        if '__' in symbol:
            logger.debug(f"Invalid symbol with double underscore: {symbol}")
            return symbol
        return symbol.replace('USDT', '-USDT').upper()
    except Exception as e:
        logger.debug(f"Error converting public symbol {symbol}: {e}")
        return symbol

def to_config_symbol(symbol):
    """
    Convert API symbol to config format (e.g., 'B-BTC_USDT' -> 'BTCUSDT').
    
    Args:
        symbol (str): API symbol.
    
    Returns:
        str: Config symbol format.
    """
    try:
        if '__' in symbol:
            logger.debug(f"Invalid symbol with double underscore: {symbol}")
            return symbol
        api_symbol = symbol.replace('B-', '') if symbol.startswith('B-') else symbol
        return api_symbol.replace('_INR', 'INR').replace('_USDT', 'USDT').upper()
    except Exception as e:
        logger.debug(f"Error converting API symbol {symbol}: {e}")
        return symbol

class CoinDCXClient:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config=None, base_url=None, api_key=None, api_secret=None):
        """
        Initialize CoinDCXClient with configuration.
        
        Args:
            config (dict): Configuration dictionary.
            base_url (str): Base API URL.
            api_key (str): API key.
            api_secret (str): API secret.
        """
        if not hasattr(self, '_initialized'):  # Prevent reinitialization
            if config is None and base_url is None:
                raise ValueError("Either config or base_url must be provided")
            self.base_url = config.get('api', {}).get('coindcx', {}).get('base_url', 'https://api.coindcx.com') if config else base_url
            self.api_key = config.get('api', {}).get('coindcx', {}).get('api_key', '') if config else api_key or ''
            self.api_secret = config.get('api', {}).get('coindcx', {}).get('api_secret', '') if config else api_secret or ''
            self.simulated = config.get('trading', {}).get('simulated', True) if config else True
            self.session = None
            self.max_retries = 3
            self.retry_delay = 2
            self.rate_limiter = asyncio.Semaphore(5)
            self.valid_pairs = set(['B-BTC_USDT', 'B-ETH_USDT', 'B-SOL_USDT', 'B-XRP_USDT', 'B-BCH_USDT',
                                    'B-LTC_USDT', 'B-DOGE_USDT', 'B-ADA_USDT']) if self.simulated else set()
            self.valid_intervals = {'1', '5', '15', '30', '60', '120', '240', '360', '720', '1D', '3D', '1W'}
            self.price_precision = {
                'B-BTC_USDT': 2, 'B-ETH_USDT': 2, 'B-SOL_USDT': 2, 'B-XRP_USDT': 2,
                'B-BCH_USDT': 2, 'B-LTC_USDT': 2, 'B-DOGE_USDT': 2, 'B-ADA_USDT': 2
            }
            self.valid_pairs_last_updated = 0
            self.valid_pairs_cache_duration = 600  # 10 minutes
            self.valid_pairs_lock = asyncio.Lock()
            self._initialized = True
            logger.info(f"CoinDCXClient initialized (simulated: {self.simulated}, base_url: {self.base_url}, valid_pairs: {self.valid_pairs})")

    async def test_connectivity(self):
        """
        Test connectivity to CoinDCX API using the active instruments endpoint.
        
        Returns:
            bool: True if connection is successful, False otherwise.
        """
        try:
            endpoint = '/exchange/v1/derivatives/futures/data/active_instruments'
            response = await self._async_request('GET', endpoint, use_public_api=True)
            if response:
                logger.info("Successfully connected to CoinDCX API")
                return True
            logger.error("API connectivity test failed: No response received")
            return False
        except Exception as e:
            logger.error(f"Failed to connect to CoinDCX API: {e}")
            return False

    async def fetch_valid_pairs(self):
        """
        Fetch valid trading pairs from CoinDCX API with caching.

        Returns:
            set: Set of valid instrument names.
        """
        async with self.valid_pairs_lock:
            current_time = time.time()
            if current_time - self.valid_pairs_last_updated < self.valid_pairs_cache_duration and self.valid_pairs:
                logger.debug("Returning cached valid pairs")
                return self.valid_pairs
            try:
                endpoint = '/exchange/v1/derivatives/futures/data/active_instruments'
                response = await self._async_request('GET', endpoint, use_public_api=True)
                if response and isinstance(response, list):
                    usdt_pairs = {pair for pair in response if isinstance(pair, str) and pair.startswith('B-') and pair.endswith('_USDT')}
                    if usdt_pairs:
                        self.valid_pairs.update(usdt_pairs)
                        tasks = []
                        for pair in usdt_pairs:
                            endpoint = "/exchange/v1/derivatives/futures/data/instrument"
                            params = {'pair': pair}
                            tasks.append(self._async_request('GET', endpoint, params=params, use_public_api=False))
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        for pair, result in zip(usdt_pairs, results):
                            if isinstance(result, Exception):
                                logger.debug(f"Error fetching details for {pair}: {result}")
                                self.price_precision[pair] = 2
                            elif result and isinstance(result, dict) and result.get('instrument'):
                                price_increment = result['instrument'].get('price_increment', 0.01)
                                precision = max(0, -int(np.floor(np.log10(float(price_increment)))))
                                self.price_precision[pair] = precision
                            else:
                                logger.debug(f"No instrument data for {pair}")
                                self.price_precision[pair] = 2
                        self.valid_pairs_last_updated = current_time
                        logger.info(f"Fetched {len(self.valid_pairs)} valid USDT futures pairs: {self.valid_pairs}")
                    else:
                        logger.warning("No USDT futures pairs found, using defaults")
                        self.valid_pairs.update(['B-BTC_USDT', 'B-ETH_USDT', 'B-SOL_USDT', 'B-XRP_USDT', 'B-BCH_USDT',
                                                'B-LTC_USDT', 'B-DOGE_USDT', 'B-ADA_USDT'])
                    return self.valid_pairs
                else:
                    logger.debug(f"Unexpected response format: {response}")
                    self.valid_pairs.update(['B-BTC_USDT', 'B-ETH_USDT', 'B-SOL_USDT', 'B-XRP_USDT', 'B-BCH_USDT',
                                            'B-LTC_USDT', 'B-DOGE_USDT', 'B-ADA_USDT'])
                    self.valid_pairs_last_updated = current_time
                    logger.info(f"Fallback to default USDT pairs: {self.valid_pairs}")
                    return self.valid_pairs
            except Exception as e:
                logger.debug(f"Error fetching valid pairs: {e}")
                self.valid_pairs.update(['B-BTC_USDT', 'B-ETH_USDT', 'B-SOL_USDT', 'B-XRP_USDT', 'B-BCH_USDT',
                                        'B-LTC_USDT', 'B-DOGE_USDT', 'B-ADA_USDT'])
                self.valid_pairs_last_updated = current_time
                logger.info(f"Fallback to default USDT pairs: {self.valid_pairs}")
                return self.valid_pairs

    async def _async_request(self, method, endpoint, params=None, body=None, use_public_api=False):
        """
        Make an async HTTP request to the API.

        Args:
            method (str): HTTP method (GET, POST).
            endpoint (str): API endpoint.
            params (dict): Query parameters.
            body (dict): Request body.
            use_public_api (bool): Whether to bypass authentication (for public endpoints).

        Returns:
            dict: API response or None if failed.
        """
        if self.simulated:
            return await self._simulate_request(method, endpoint, params, body)
        if not self.session:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        headers = {'Content-Type': 'application/json'}
        base_url = 'https://public.coindcx.com' if endpoint.startswith('/market_data') else self.base_url
        url = f"{base_url}{endpoint}"

        # Prepare body and signature
        body = body or {}
        if method in ['POST', 'GET'] and not use_public_api and self.api_key and self.api_secret:
            body['timestamp'] = int(time.time() * 1000)
            body_str = json.dumps(body, separators=(',', ':'))
            signature = hmac.new(
                self.api_secret.encode('utf-8'),
                body_str.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            headers.update({
                'X-AUTH-APIKEY': self.api_key,
                'X-AUTH-SIGNATURE': signature
            })

        for attempt in range(self.max_retries):
            try:
                async with self.rate_limiter:
                    if method == 'GET':
                        async with self.session.get(url, params=params, headers=headers) as response:
                            response_text = await response.text()
                            if response.status == 200:
                                return await response.json()
                            else:
                                logger.warning(f"HTTP error for {url} (attempt {attempt + 1}/{self.max_retries}): {response.status} - {response_text}")
                                logger.debug(f"Request details: method={method}, params={params}, headers={headers}")
                                logger.debug(f"Full response: {response_text}")
                                if response.status == 429:
                                    delay = self.retry_delay * (2 ** attempt) + random.uniform(0.1, 0.5)
                                    logger.info(f"Rate limit hit, retrying after {delay:.2f} seconds")
                                    await asyncio.sleep(delay)
                                elif response.status in [400, 403, 404]:
                                    logger.debug(f"Invalid request or resource not found: {url} with params={params}")
                                    return None
                                else:
                                    raise Exception(f"API error: {response.status} - {response_text}")
                    elif method == 'POST':
                        async with self.session.post(url, json=body, headers=headers) as response:
                            response_text = await response.text()
                            if response.status == 200:
                                return await response.json()
                            else:
                                logger.warning(f"HTTP error for {url} (attempt {attempt + 1}/{self.max_retries}): {response.status} - {response_text}")
                                logger.debug(f"Request details: method={method}, body={body}, headers={headers}")
                                logger.debug(f"Full response: {response_text}")
                                if response.status == 429:
                                    delay = self.retry_delay * (2 ** attempt) + random.uniform(0.1, 0.5)
                                    logger.info(f"Rate limit hit, retrying after {delay:.2f} seconds")
                                    await asyncio.sleep(delay)
                                elif response.status in [400, 403, 404]:
                                    logger.debug(f"Invalid request or resource not found: {url}")
                                    return None
                                else:
                                    raise Exception(f"API error: {response.status} - {response_text}")
            except aiohttp.ClientError as e:
                logger.debug(f"Network error in request (attempt {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))
                else:
                    logger.error(f"Max retries reached for {url}: {e}")
                    return None
            except asyncio.TimeoutError:
                logger.debug(f"Timeout in request (attempt {attempt + 1}/{self.max_retries}): {url}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))
                else:
                    logger.error(f"Request timed out after {self.max_retries} attempts: {url}")
                    return None
        logger.warning(f"Max retries reached for {url}")
        return None

    async def _simulate_request(self, method, endpoint, params, body):
        """
        Simulate API requests for testing.
        
        Args:
            method (str): HTTP method.
            endpoint (str): API endpoint.
            params (dict): Query parameters.
            body (dict): Request body.
        
        Returns:
            dict: Simulated response.
        """
        try:
            if endpoint == '/exchange/v1/derivatives/futures/data/active_instruments':
                logger.debug("Simulating market pairs request")
                return ['B-BTC_USDT', 'B-ETH_USDT', 'B-SOL_USDT', 'B-XRP_USDT', 'B-BCH_USDT',
                        'B-LTC_USDT', 'B-DOGE_USDT', 'B-ADA_USDT']
            elif endpoint == '/market_data/candlesticks':
                symbol = params.get('pair', 'B-BTC_USDT')
                timeframe = params.get('resolution', '1')
                limit = params.get('limit', 50)
                if symbol not in self.valid_pairs:
                    logger.debug(f"Invalid or unsupported pair: {symbol}. Available pairs: {self.valid_pairs}")
                    return None
                if timeframe not in self.valid_intervals:
                    logger.debug(f"Invalid timeframe: {timeframe}. Valid intervals: {self.valid_intervals}")
                    return None
                logger.debug(f"Simulating candlestick data for {symbol} ({timeframe})")
                base_price = 108510 if 'BTC' in symbol else 2427 if 'ETH' in symbol else 100
                return {
                    's': 'ok',
                    'data': [
                        {
                            'time': int(time.time() * 1000) - (i * 60 * 1000),
                            'open': base_price + np.random.normal(0, base_price * 0.002),
                            'high': base_price + np.random.normal(0, base_price * 0.003),
                            'low': base_price + np.random.normal(0, base_price * 0.003),
                            'close': base_price + np.random.normal(0, base_price * 0.002),
                            'volume': 10 + np.random.normal(0, 2)
                        } for i in range(limit)
                    ]
                }
            # ... rest of _simulate_request unchanged ...
        except Exception as e:
            logger.debug(f"Error in simulated request: {e}")
            return None

    async def fetch_candlestick_data(self, symbol, timeframe, limit=50):
        """
        Fetch candlestick data for a given symbol and timeframe.
        
        Args:
            symbol (str): Trading pair.
            timeframe (str): Timeframe (e.g., '1m', '5m', '1h').
            limit (int): Number of data points.
        
        Returns:
            dict: Candlestick data or None if failed.
        """
        try:
            api_symbol = to_api_symbol(symbol, is_futures=True)
            if api_symbol not in self.valid_pairs:
                logger.warning(f"Invalid or unsupported pair: {api_symbol}. Available pairs: {list(self.valid_pairs)[:10]}")
                return None
            resolution = {
                '1m': '1', '5m': '5', '15m': '15', '30m': '30', '1h': '60',
                '2h': '120', '4h': '240', '6h': '360', '12h': '720',
                '1d': '1D', '3d': '3D', '1w': '1W'
            }.get(timeframe, '1')
            if resolution not in self.valid_intervals:
                logger.debug(f"Invalid timeframe: {timeframe}. Valid intervals: {self.valid_intervals}")
                return None
            end_time = int(datetime.now().timestamp())
            start_time = end_time - (limit * 60 * int(resolution.replace('D', '1440').replace('W', '10080')) * 5)  # 5x range
            params = {
                'pair': api_symbol,
                'resolution': resolution,
                'from': start_time,
                'to': end_time,
                'pcode': 'f'
            }
            logger.debug(f"Candlestick request: symbol={api_symbol}, resolution={resolution}, limit={limit}, from={start_time}, to={end_time}")
            response = await self._async_request('GET', endpoint='/market_data/candlesticks', params=params, use_public_api=True)
            if response and isinstance(response, dict) and response.get('s') == 'ok':
                logger.info(f"Fetched {len(response.get('data', []))} candlesticks for {symbol} ({timeframe})")
                return response
            logger.warning(f"Failed to fetch candlestick data for {symbol} ({timeframe}): {response}")
            # Fallback to 5m if 1m fails
            if resolution == '1':
                logger.info(f"Retrying with 5m resolution for {symbol}")
                return await self.fetch_candlestick_data(symbol, '5m', limit)
            return None
        except Exception as e:
            logger.debug(f"Error fetching candlestick data for {symbol} ({timeframe}): {e}")
            return None

    async def create_order(self, symbol, side, quantity, price=None, leverage=None, order_type="market_order"):
        try:
            if side.lower() not in ['buy', 'sell']:
                raise ValueError(f"Invalid side: {side}")
            if order_type.lower() not in ['market_order', 'limit_order', 'stop_limit', 'stop_market', 'take_profit_limit', 'take_profit_market']:
                raise ValueError(f"Invalid order_type: {order_type}")
            if symbol is None or '__' in symbol:
                logger.error(f"Invalid symbol format: {symbol}")
                return None
            if symbol not in self.valid_pairs:
                logger.info(f"Symbol {symbol} not in valid pairs, refreshing...")
                await self.fetch_valid_pairs()
                if symbol not in self.valid_pairs:
                    logger.warning(f"Invalid or unsupported pair: {symbol}. Available pairs: {list(self.valid_pairs)[:10]}")
                    return None
            body = {
                "order": {
                    "pair": symbol,
                    "side": side.lower(),
                    "total_quantity": str(quantity),
                    "order_type": order_type.lower(),
                    "notification": "no_notification",
                    "time_in_force": "good_till_cancel"
                }
            }
            if price and order_type in ["limit_order", "stop_limit", "take_profit_limit"]:
                precision = self.price_precision.get(symbol, 2)
                body["order"]["price"] = str(round(float(price), precision))
            if leverage:
                body["order"]["leverage"] = str(leverage)
            response = await self._async_request('POST', endpoint='/exchange/v1/derivatives/futures/orders', body=body)
            if response is None:
                logger.error(f"Order creation failed for {symbol}: API returned None")
                return None
            if isinstance(response, dict):
                logger.info(f"Order created: {symbol} {side} {quantity} @ {price or 'market'}")
                return response
            logger.error(f"Order creation failed: Unexpected response {response}")
            return None
        except Exception as e:
            logger.error(f"Error creating order for {symbol}: {e}", exc_info=True)
            return None

async def fetch_valid_pairs(self):
    async with self.valid_pairs_lock:
        current_time = time.time()
        if current_time - self.valid_pairs_last_updated < self.valid_pairs_cache_duration and self.valid_pairs:
            logger.debug("Returning cached valid pairs")
            return self.valid_pairs
        try:
            endpoint = '/exchange/v1/derivatives/futures/data/active_instruments'
            response = await self._async_request('GET', endpoint, use_public_api=True)
            logger.debug(f"Valid pairs response: {response}")
            if response and isinstance(response, list):
                usdt_pairs = {pair for pair in response if isinstance(pair, str) and pair.startswith('B-') and pair.endswith('_USDT')}
                if not usdt_pairs:
                    logger.warning("No USDT futures pairs found, using fallback")
                    usdt_pairs = {'B-BTC_USDT', 'B-ETH_USDT', 'B-SOL_USDT', 'B-XRP_USDT', 'B-BCH_USDT', 'B-LTC_USDT', 'B-DOGE_USDT', 'B-ADA_USDT'}
                self.valid_pairs = usdt_pairs
                self.valid_pairs_last_updated = current_time
                logger.info(f"Fetched {len(self.valid_pairs)} valid USDT futures pairs")
                return self.valid_pairs
            else:
                logger.error(f"Invalid response format: {response}")
                self.valid_pairs = {'B-BTC_USDT', 'B-ETH_USDT', 'B-SOL_USDT', 'B-XRP_USDT', 'B-BCH_USDT', 'B-LTC_USDT', 'B-DOGE_USDT', 'B-ADA_USDT'}
                self.valid_pairs_last_updated = current_time
                logger.info(f"Using fallback valid pairs: {self.valid_pairs}")
                return self.valid_pairs
        except Exception as e:
            logger.error(f"Error fetching valid pairs: {e}", exc_info=True)
            self.valid_pairs = {'B-BTC_USDT', 'B-ETH_USDT', 'B-SOL_USDT', 'B-XRP_USDT', 'B-BCH_USDT', 'B-LTC_USDT', 'B-DOGE_USDT', 'B-ADA_USDT'}
            self.valid_pairs_last_updated = current_time
            logger.info(f"Using fallback valid pairs after error: {self.valid_pairs}")
            return self.valid_pairs
    async def create_tpsl(self, symbol, side, quantity, take_profit, stop_loss, leverage=None):
        """
        Create take-profit and stop-loss orders for a position.
        
        Args:
            symbol (str): Trading pair.
            side (str): 'buy' or 'sell'.
            quantity (float): Position quantity.
            take_profit (float): Take-profit price.
            stop_loss (float): Stop-loss price.
            leverage (int): Leverage.
        
        Returns:
            dict: TP/SL response or None if failed.
        """
        try:
            if side.lower() not in ['buy', 'sell']:
                raise ValueError(f"Invalid side: {side}")
            api_symbol = to_api_symbol(symbol, is_futures=True)
            if api_symbol not in self.valid_pairs:
                logger.warning(f"Invalid or unsupported pair: {api_symbol}. Available pairs: {list(self.valid_pairs)[:10]}")
                return None
            take_profit = float(take_profit)
            stop_loss = float(stop_loss)
            precision = self.price_precision.get(api_symbol, 2)
            take_profit = round(take_profit, precision)
            stop_loss = round(stop_loss, precision)
            position = await self.get_position(api_symbol)
            entry_price = float(position[0].get('entry_price', 0)) if position and isinstance(position, list) and len(position) > 0 else None
            if entry_price:
                min_distance = entry_price * 0.005
                if side.lower() == 'buy':
                    if stop_loss >= entry_price or (entry_price - stop_loss) < min_distance:
                        logger.debug(f"Invalid stop_loss {stop_loss} for {api_symbol}: Must be below {entry_price - min_distance}")
                        return None
                    if take_profit <= entry_price or (take_profit - entry_price) < min_distance:
                        logger.debug(f"Invalid take_profit {take_profit} for {api_symbol}: Must be above {entry_price + min_distance}")
                        return None
                else:
                    if stop_loss <= entry_price or (stop_loss - entry_price) < min_distance:
                        logger.debug(f"Invalid stop_loss {stop_loss} for {api_symbol}: Must be above {entry_price + min_distance}")
                        return None
                    if take_profit >= entry_price or (entry_price - take_profit) < min_distance:
                        logger.debug(f"Invalid take_profit {take_profit} for {api_symbol}: Must be below {entry_price - min_distance}")
                        return None
            if self.simulated:
                logger.debug(f"Simulating TP/SL for {symbol} {side}: TP={take_profit}, SL={stop_loss}")
                return {
                    'take_profit': {
                        'id': f"sim_tp_{int(time.time())}",
                        'pair': api_symbol,
                        'stop_price': str(take_profit),
                        'order_type': 'take_profit_limit'
                    },
                    'stop_loss': {
                        'id': f"sim_sl_{int(time.time())}",
                        'pair': api_symbol,
                        'stop_price': str(stop_loss),
                        'order_type': 'stop_limit'
                    }
                }
            endpoint = "/exchange/v1/derivatives/futures/positions/create_tpsl"
            position_id = position[0].get('id') if position and isinstance(position, list) and len(position) > 0 else None
            if not position_id:
                logger.debug(f"No active position found for {api_symbol}")
                return None
            body = {
                "timestamp": int(time.time() * 1000),
                "id": position_id,
                "take_profit": {
                    "stop_price": str(take_profit),
                    "limit_price": str(take_profit),
                    "order_type": "take_profit_limit"
                },
                "stop_loss": {
                    "stop_price": str(stop_loss),
                    "limit_price": str(stop_loss),
                    "order_type": "stop_limit"
                }
            }
            if leverage:
                body["leverage"] = str(leverage)
            response = await self._async_request("POST", endpoint, body=body)
            if response and isinstance(response, dict) and ('stop_loss' in response or 'take_profit' in response):
                logger.info(f"TP/SL orders created: {symbol} {side} TP={take_profit} SL={stop_loss}")
                return response
            logger.debug(f"TP/SL creation failed: {response}")
            return None
        except Exception as e:
            logger.debug(f"Error creating TP/SL for {symbol}: {e}")
            return None

    async def get_position(self, symbol=None):
        """
        Fetch position data for a symbol or all positions.
        
        Args:
            symbol (str): Trading pair (optional).
        
        Returns:
            list: Position data or empty list if failed.
        """
        try:
            endpoint = "/exchange/v1/derivatives/futures/positions"
            body = {"timestamp": int(time.time() * 1000), "pair": to_api_symbol(symbol, is_futures=True)} if symbol else {"timestamp": int(time.time() * 1000)}
            response = await self._async_request("POST", endpoint, body=body)
            if response and isinstance(response, list):
                logger.info(f"Position fetched for {symbol or 'all'}")
                return response
            logger.debug(f"Position fetch failed: {response}")
            return []
        except Exception as e:
            logger.debug(f"Error fetching position for {symbol or 'all'}: {e}")
            return []

    async def fetch_futures_balance(self, currency='INR'):
        """
        Fetch futures balance for a currency.
        
        Args:
            currency (str): Currency (e.g., 'INR', 'USDT').
        
        Returns:
            dict: Balance details.
        """
        try:
            endpoint = '/exchange/v1/users/balances'
            body = {"timestamp": int(time.time() * 1000)}
            if self.simulated:
                logger.debug(f"Simulating futures balance for {currency}")
                return {
                    'currency': currency,
                    'available_balance': 1000.0 if currency == 'INR' else 1000.0 / 86,
                    'locked_balance': 0.0
                }
            response = await self._async_request('POST', endpoint, body=body)
            if response and isinstance(response, dict) and response.get('data'):
                for balance in response.get('data', []):
                    if balance.get('currency') == currency:
                        available = float(balance.get('balance', 0.0)) - float(balance.get('locked_balance', 0.0))
                        locked = float(balance.get('locked_balance', 0.0))
                        logger.info(f"Futures balance fetched: {currency} available={available} locked={locked}")
                        return {
                            'currency': currency,
                            'available_balance': available,
                            'locked_balance': locked
                        }
                logger.warning(f"No balance found for {currency}")
                return {'currency': currency, 'available_balance': 0.0, 'locked_balance': 0.0}
            logger.debug(f"Futures balance failed: {response}")
            return {'currency': currency, 'available_balance': 0.0, 'locked_balance': 0.0}
        except Exception as e:
            logger.debug(f"Error fetching futures balance for {currency}: {e}")
            return {'currency': currency, 'available_balance': 0.0, 'locked_balance': 0.0}

    async def get_balance(self, currency='INR'):
        """
        Fetch balance for a currency.
        
        Args:
            currency (str): Currency (e.g., 'INR', 'USDT').
        
        Returns:
            float: Balance amount.
        """
        try:
            endpoint = '/exchange/v1/users/balances'
            body = {"timestamp": int(time.time() * 1000)}
            response = await self._async_request('POST', endpoint, body=body)
            if response and isinstance(response, dict) and response.get('data'):
                for balance in response.get('data', []):
                    if balance.get('currency') == currency:
                        logger.info(f"Balance fetched: {currency} {balance.get('balance', 0.0)}")
                        return float(balance.get('balance', 0.0))
                logger.warning(f"No balance found for {currency}")
                return 0.0
            logger.debug(f"Balance fetch failed: {response}")
            return 0.0
        except Exception as e:
            logger.debug(f"Error fetching balance for {currency}: {e}")
            return 0.0

    async def close(self):
        """
        Close the HTTP session.
        """
        if self.session:
            await self.session.close()
            self.session = None
            logger.info("CoinDCXClient session closed")