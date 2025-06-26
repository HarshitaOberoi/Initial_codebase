import asyncio
import yaml
from datetime import datetime
from src.utils.logger import setup_logger
from src.data.fetcher import DataFetcher
from src.strategies.strategy_manager import StrategyManager
from src.trading.executor import TradeExecutor
from src.utils.websocket import WebSocketClient
from src.exchange.coindcx_client import CoinDCXClient, to_api_symbol

logger = setup_logger()

async def log_performance(executor):
    while True:
        try:
            executor._log_trade_summary()
            await asyncio.sleep(3600)
        except Exception as error:
            logger.error(f"Error logging performance: {error}")

async def log_websocket_data(websocket_client):
    while True:
        try:
            for key, df in websocket_client.candle_buffer.items():
                if not df.empty:
                    logger.info(f"Real-time candle buffer for {key}:\n{df.tail(3).to_string(index=False)}")
            await asyncio.sleep(30)
        except Exception as error:
            logger.error(f"Error logging WebSocket data: {error}")
        await asyncio.sleep(30)

async def main():
    config_path = "config/config.yaml"
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.debug(f"Config loaded: {config}")
    except Exception as error:
        logger.error(f"Failed to load config from {config_path}: {error}")
        return

    logger.debug("Initializing CoinDCXClient")
    client = CoinDCXClient(config=config)
    
    if not await client.test_connectivity():
        logger.error("Cannot connect to CoinDCX API. Exiting.")
        await client.close()
        return
    
    valid_pairs = None
    for attempt in range(3):
        try:
            valid_pairs = await client.fetch_valid_pairs()
            logger.info(f"Fetched {len(valid_pairs)} valid USDT futures pairs")
            logger.debug(f"Sample valid pairs: {list(valid_pairs)[:10]}")
            if valid_pairs:
                break
            logger.warning(f"Retry {attempt + 1}/3 for fetch_valid_pairs: No pairs returned")
            await asyncio.sleep(1)
        except Exception as error:
            logger.warning(f"Retry {attempt + 1}/3 for fetch_valid_pairs: {error}")
            await asyncio.sleep(1)
    if not valid_pairs:
        logger.error("No valid pairs available after retries. Exiting.")
        await client.close()
        return

    simulated = config.get('trading', {}).get('simulated', True)
    initial_balance = config.get('trading', {}).get('initial_balance', 1000)
    symbols = [config.get('trading', {}).get('symbol', 'BTCUSDT')] + config.get('trading', {}).get('additional_symbols', [])
    timeframes = config.get('trading', {}).get('timeframes', ['1m'])

    valid_symbols = []
    for s in symbols:
        api_symbol = to_api_symbol(s, is_futures=True)
        if api_symbol is None or '__' in api_symbol or api_symbol not in valid_pairs:
            logger.warning(f"Skipping invalid symbol {s} (API: {api_symbol})")
            continue
        valid_symbols.append(s)
    supported_timeframes = {'1m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d', '3d', '1w'}
    valid_timeframes = [tf for tf in timeframes if tf in supported_timeframes]
    
    if not valid_symbols or not valid_timeframes:
        logger.error(f"No valid symbols or timeframes. Symbols: {symbols}, Timeframes: {timeframes}, Valid Pairs: {list(valid_pairs)[:10]}")
        await client.close()
        return

    logger.info(f"Starting bot in {'simulated' if simulated else 'live'} mode: "
                f"Balance={initial_balance:.2f} USDT, Symbols={valid_symbols}, Timeframes={valid_timeframes}")

    fetcher = None
    executor = None
    strategy_manager = None
    websocket_client = None

    try:
        logger.debug("Initializing DataFetcher")
        fetcher = DataFetcher(config)
        fetcher.valid_pairs = valid_pairs
        logger.debug("Initializing TradeExecutor")
        executor = TradeExecutor(config, client=client)
        logger.debug("Initializing StrategyManager")
        strategy_manager = StrategyManager(config)
        strategy_manager.set_fetcher(fetcher)
        strategy_manager.set_executor(executor)
        logger.debug("Initializing WebSocketClient")
        websocket_client = WebSocketClient(config, fetcher=fetcher, strategy_manager=strategy_manager, executor=executor)
        
        if not all([fetcher, executor, strategy_manager, websocket_client]):
            logger.error("One or more components failed to initialize")
            await client.close()
            return
        
        logger.info("Pre-fetching initial candlestick data...")
        tasks = [
            fetcher.fetch_data(symbol, timeframe, limit=35)
            for symbol in valid_symbols
            for timeframe in valid_timeframes
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        fetched_data = {}
        for i, (symbol, timeframe) in enumerate([(s, t) for s in valid_symbols for t in valid_timeframes]):
            if isinstance(results[i], Exception):
                logger.error(f"Failed to fetch data for {symbol} ({timeframe}): {results[i]}")
                continue
            elif results[i].empty:
                logger.warning(f"No data fetched for {symbol} ({timeframe})")
                continue
            else:
                logger.info(f"Fetched {len(results[i])} rows for {symbol} ({timeframe})")
                logger.debug(f"Data for {symbol} ({timeframe}):\n{results[i].tail(3).to_string(index=False)}")
                fetched_data[(symbol, timeframe)] = results[i]

        logger.info("Proceeding with WebSocket for real-time data")
        executor._log_trade_summary()
        asyncio.create_task(log_performance(executor))
        await websocket_client.start()
    except Exception as error:
        logger.error(f"Initialization failed: {error}", exc_info=True)
    finally:
        if websocket_client:
            try:
                await websocket_client.stop()
            except Exception as error:
                logger.error(f"Error stopping WebSocket client: {error}")
        if executor:
            try:
                await executor.close_all_trades()
            except Exception as error:
                logger.error(f"Error closing trades: {error}")
        if fetcher and hasattr(fetcher, 'client') and fetcher.client:
            try:
                await fetcher.close()
                logger.debug("Fetcher client session closed")
            except Exception as error:
                logger.error(f"Error closing fetcher client: {error}")
        if client:
            try:
                await client.close()
                logger.debug("CoinDCXClient session closed")
            except Exception as error:
                logger.error(f"Error closing CoinDCXClient: {error}")
        logger.info("Bot shutdown completed")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as error:
        logger.error(f"Unexpected error: {error}", exc_info=True)