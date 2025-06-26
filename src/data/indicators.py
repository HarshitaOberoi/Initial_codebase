import pandas as pd
import numpy as np
from src.utils.logger import setup_logger

logger = setup_logger()

class Indicators:
    def __init__(self, config):
        self.config = config
        self.rsi_period = self.config.get('indicators', {}).get('rsi', {}).get('period', 14)
        self.stoch_k = self.config.get('indicators', {}).get('stochastic', {}).get('k', 14)
        self.stoch_d = self.config.get('indicators', {}).get('stochastic', {}).get('d', 3)
        self.stoch_smooth = self.config.get('indicators', {}).get('stochastic', {}).get('smooth', 3)
        self.macd_fast = self.config.get('indicators', {}).get('macd', {}).get('fast', 12)
        self.macd_slow = self.config.get('indicators', {}).get('macd', {}).get('slow', 26)
        self.macd_signal = self.config.get('indicators', {}).get('macd', {}).get('signal', 9)
        self.bollinger_period = self.config.get('indicators', {}).get('bollinger', {}).get('period', 20)
        self.bollinger_std = self.config.get('indicators', {}).get('bollinger', {}).get('std', 2)
        self.atr_period = self.config.get('indicators', {}).get('atr', {}).get('period', 14)
        self.adx_period = self.config.get('indicators', {}).get('adx', {}).get('period', 14)
        self.volume_sma_period = self.config.get('indicators', {}).get('volume_sma', {}).get('period', 20)

    def calculate_rsi(self, data, period=None):
        period = period or self.rsi_period
        delta = data['close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        rs = avg_gain / avg_loss
        data['rsi'] = 100 - (100 / (1 + rs))
        return data

    def calculate_stochastic(self, data):
        low = data['low'].rolling(window=self.stoch_k).min()
        high = data['high'].rolling(window=self.stoch_k).max()
        data['stoch_k'] = 100 * (data['close'] - low) / (high - low)
        data['stoch_d'] = data['stoch_k'].rolling(window=self.stoch_d).mean()
        return data

    def calculate_macd(self, data):
        ema_fast = data['close'].ewm(span=self.macd_fast, adjust=False).mean()
        ema_slow = data['close'].ewm(span=self.macd_slow, adjust=False).mean()
        data['macd'] = ema_fast - ema_slow
        data['macd_signal'] = data['macd'].ewm(span=self.macd_signal, adjust=False).mean()
        data['macd_histogram'] = data['macd'] - data['macd_signal']
        return data

    def calculate_bollinger_bands(self, data):
        sma = data['close'].rolling(window=self.bollinger_period).mean()
        std = data['close'].rolling(window=self.bollinger_period).std()
        data['bollinger_mid'] = sma
        data['bollinger_upper'] = sma + (std * self.bollinger_std)
        data['bollinger_lower'] = sma - (std * self.bollinger_std)
        return data

    def calculate_atr(self, data):
        high_low = data['high'] - data['low']
        high_close = np.abs(data['high'] - data['close'].shift())
        low_close = np.abs(data['low'] - data['close'].shift())
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        data['atr'] = tr.rolling(window=self.atr_period).mean()
        return data

    def calculate_adx(self, data):
        high_diff = data['high'].diff()
        low_diff = -data['low'].diff()
        plus_dm = np.where((high_diff > low_diff) & (high_diff > 0), high_diff, 0)
        minus_dm = np.where((low_diff > high_diff) & (low_diff > 0), low_diff, 0)
        tr = self.calculate_atr(data.copy())['atr']
        plus_di = 100 * pd.Series(plus_dm).rolling(window=self.adx_period).mean() / tr
        minus_di = 100 * pd.Series(minus_dm).rolling(window=self.adx_period).mean() / tr
        dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di)
        data['adx'] = dx.rolling(window=self.adx_period).mean()
        return data

    def calculate_volume_sma(self, data):
        data['volume_sma'] = data['volume'].rolling(window=self.volume_sma_period).mean()
        return data

    def calculate_ema(self, data, period, column='close'):
        data[f'ema_{period}'] = data[column].ewm(span=period, adjust=False).mean()
        return data

    def calculate_all(self, data):
        try:
            if data.empty or len(data) < 35:
                logger.debug(f"Insufficient data for indicators: Rows={len(data)}")
                return data
            df = data.copy()
            df = self.calculate_rsi(df)
            df = self.calculate_stochastic(df)
            df = self.calculate_macd(df)
            df = self.calculate_bollinger_bands(df)
            df = self.calculate_atr(df)
            df = self.calculate_adx(df)
            df = self.calculate_volume_sma(df)
            df = self.calculate_ema(df, 20)
            df = self.calculate_ema(df, 50)
            df = self.calculate_ema(df, self.macd_fast, column='close')  # ema_fast for TrendStrategy
            df = self.calculate_ema(df, self.macd_slow, column='close')  # ema_slow for TrendStrategy
            # Rename to match TrendStrategy expectations
            df = df.rename(columns={f'ema_{self.macd_fast}': 'ema_fast', f'ema_{self.macd_slow}': 'ema_slow'})
            # Handle NaNs
            for col in ['rsi', 'stoch_k', 'stoch_d', 'macd', 'macd_signal', 'macd_histogram', 
                        'bollinger_mid', 'bollinger_upper', 'bollinger_lower', 'atr', 'adx', 
                        'volume_sma', 'ema_20', 'ema_50', 'ema_fast', 'ema_slow']:
                if col in df.columns:
                    df[col] = df[col].ffill().bfill()
            logger.info(f"Calculated indicators: Columns={df.columns.tolist()}, NaNs={df.isna().sum().to_dict()}")
            logger.debug(f"Last row indicators: {df.iloc[-1].to_dict()}")
            return df
        except Exception as e:
            logger.error(f"Error calculating indicators: {e}")
            return data