from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class BaseStrategy(ABC):
    """Base class for all trading strategies"""
    
    def __init__(self, name: str):
        """Initialize strategy
        
        Args:
            name: Strategy name
        """
        self.name = name
        self.position = {}  # Current positions
        self.params = {}    # Strategy parameters
        self._last_update_time = None
        self.performance_metrics = {}
        
    @abstractmethod
    def initialize(self, **kwargs):
        """Initialize strategy parameters"""
        pass
    
    @abstractmethod
    def generate_signals(self, data: pd.DataFrame) -> Dict[str, float]:
        """Generate trading signals based on input data
        
        Args:
            data: Input market data with datetime index
            
        Returns:
            Dictionary containing trading signals for each symbol
        """
        pass
    
    def calculate_position_size(self, signals: Dict[str, float], 
                              total_capital: float,
                              max_position_size: float = 0.1,
                              position_sizing: str = 'equal') -> Dict[str, float]:
        """Calculate position sizes based on signals
        
        Args:
            signals: Trading signals dictionary
            total_capital: Total capital to allocate
            max_position_size: Maximum position size as fraction of total capital
            position_sizing: Position sizing method ('equal', 'volatility', 'kelly')
            
        Returns:
            Dictionary of position sizes for each symbol
        """
        if not signals:
            return {}
            
        positions = {}
        n_signals = len(signals)
        
        if position_sizing == 'equal':
            # Equal position sizing
            position_size = min(1.0 / n_signals, max_position_size)
            position_value = position_size * total_capital
            
            for symbol, signal in signals.items():
                positions[symbol] = position_value * signal
                
        elif position_sizing == 'volatility':
            # Volatility-based position sizing
            if not hasattr(self, '_volatilities'):
                logger.warning("Volatilities not calculated. Using equal position sizing.")
                return self.calculate_position_size(signals, total_capital, max_position_size, 'equal')
                
            total_vol = sum(self._volatilities.values())
            for symbol, signal in signals.items():
                vol_weight = self._volatilities.get(symbol, 1.0) / total_vol
                position_size = min(vol_weight, max_position_size)
                positions[symbol] = position_size * total_capital * signal
                
        elif position_sizing == 'kelly':
            # Kelly criterion-based sizing
            if not hasattr(self, '_win_rates') or not hasattr(self, '_profit_ratios'):
                logger.warning("Kelly metrics not calculated. Using equal position sizing.")
                return self.calculate_position_size(signals, total_capital, max_position_size, 'equal')
                
            for symbol, signal in signals.items():
                win_rate = self._win_rates.get(symbol, 0.5)
                profit_ratio = self._profit_ratios.get(symbol, 1.0)
                kelly_fraction = win_rate - (1 - win_rate) / profit_ratio
                position_size = min(max(0, kelly_fraction), max_position_size)
                positions[symbol] = position_size * total_capital * signal
                
        return positions
    
    def update_positions(self, positions: Dict[str, float], timestamp: Optional[datetime] = None):
        """Update strategy positions
        
        Args:
            positions: New position sizes dictionary
            timestamp: Update timestamp
        """
        self.position = positions.copy()
        self._last_update_time = timestamp or datetime.now()
        
    def get_positions(self) -> Dict[str, float]:
        """Get current positions
        
        Returns:
            Dictionary of current positions
        """
        return self.position.copy()
        
    def calculate_metrics(self, data: pd.DataFrame) -> Dict[str, float]:
        """Calculate strategy performance metrics
        
        Args:
            data: Historical price data
            
        Returns:
            Dictionary of performance metrics
        """
        if len(self.position) == 0:
            return {}
            
        returns = data.pct_change()
        position_df = pd.DataFrame(self.position, index=[0])
        
        # Calculate portfolio returns
        portfolio_returns = (returns * position_df.iloc[0]).sum(axis=1)
        
        # Basic metrics
        metrics = {
            'total_return': (1 + portfolio_returns).prod() - 1,
            'annual_return': (1 + portfolio_returns).prod() ** (252/len(returns)) - 1,
            'volatility': portfolio_returns.std() * np.sqrt(252),
            'sharpe_ratio': portfolio_returns.mean() / portfolio_returns.std() * np.sqrt(252),
            'max_drawdown': (portfolio_returns.cumsum() - portfolio_returns.cumsum().cummax()).min()
        }
        
        self.performance_metrics = metrics
        return metrics
        
    def set_volatilities(self, volatilities: Dict[str, float]):
        """Set asset volatilities for position sizing
        
        Args:
            volatilities: Dictionary of asset volatilities
        """
        self._volatilities = volatilities
        
    def set_kelly_metrics(self, win_rates: Dict[str, float], profit_ratios: Dict[str, float]):
        """Set Kelly criterion metrics for position sizing
        
        Args:
            win_rates: Dictionary of historical win rates
            profit_ratios: Dictionary of profit/loss ratios
        """
        self._win_rates = win_rates
        self._profit_ratios = profit_ratios
