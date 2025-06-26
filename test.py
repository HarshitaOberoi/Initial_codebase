import asyncio
from src.data.fetcher import DataFetcher
from src.exchange.coindcx_client import CoinDCXClient, to_api_symbol
from src.trading.executor import TradeExecutor
from src.strategies.strategy_manager import StrategyManager
from src.utils.logger import setup_logger

logger = setup_logger()

async def test_bot():
    config_path = "config/config.yaml"
    try:
        fetcher = DataFetcher(config_path)
        client = CoinDCXClient(config_path)
        executor = TradeExecutor(config_path, client=client)
        strategy_manager = StrategyManager(config_path)
        strategy_manager.set_executor(executor)
        
        symbols = ["LTCUSDT", "ADAUSDT"]
        timeframes = ["1m", "5m", "1h"]
        
        for symbol in symbols:
            for timeframe in timeframes:
                logger.info(f"Testing {symbol} on {timeframe}")
                data = await fetcher.fetch_data(symbol, timeframe, limit=175)
                logger.info(f"Fetched data for {symbol} ({timeframe}): Rows={len(data)}, Columns={data.columns.tolist()}, NaNs={data.isna().sum().to_dict()}")
                logger.debug(f"Last 2 rows:\n{data.tail(2).to_string()}")
                
                signal, params = await strategy_manager.process_data(symbol, timeframe, data)
                logger.info(f"Signal for {symbol} ({timeframe}): {signal}, Params: {params}")
                
                if signal in ['buy', 'sell']:
                    api_symbol = to_api_symbol(symbol, is_futures=True)
                    success = await executor.execute_trade(api_symbol, timeframe, signal, params, params.get('strategy', 'unknown'))
                    logger.info(f"Trade execution for {api_symbol} ({timeframe}): {'Success' if success else 'Failed'}")
    except Exception as e:
        logger.error(f"Test bot error: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(test_bot())