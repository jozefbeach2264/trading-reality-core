# dan2.py
import pandas as pd
import numpy as np
import logging
import time

logger = logging.getLogger(__name__)

class Dan2:
    def __init__(self, hub):
        self.hub = hub

    def calculate_momentum(self, data):
        """Calculate momentum indicators."""
        try:
            logger.info("Dan2: Calculating momentum...")
            # Example: Simple momentum (price change over n periods)
            data['momentum'] = data['close'].pct_change(periods=10) * 100
            logger.info("Dan2: Momentum calculation complete.")
            return data
        except Exception as e:
            logger.error(f"Dan2: Error in momentum calculation: {e}")
            raise

    def generate_trade_signals(self, data, traps):
        """Generate trade signals based on Dan1's traps and momentum."""
        try:
            logger.info("Dan2: Generating trade signals...")
            # Example: Buy signal if momentum > 0 and no trap
            data['trade_signal'] = np.where(
                (data['momentum'] > 0) & (~data.index.isin(traps.index)),
                'BUY', 'HOLD'
            )
            logger.info(f"Dan2: Generated trade signals: {data['trade_signal'].value_counts().to_dict()}")
            return data
        except Exception as e:
            logger.error(f"Dan2: Error in trade signal generation: {e}")
            raise

    def run(self, data):
        """Main execution loop for Dan2."""
        try:
            # Wait for Dan1 results
            logger.info("Dan2: Waiting for Dan1 results...")
            while not self.hub.has_data('dan1_results'):
                time.sleep(1)

            # Retrieve Dan1 results
            dan1_results = self.hub.get_data('dan1_results')
            processed_data = dan1_results['processed_data']
            traps = dan1_results['traps']

            # Perform momentum analysis
            momentum_data = self.calculate_momentum(processed_data)

            # Generate trade signals
            trade_signals = self.generate_trade_signals(momentum_data, traps)

            # Store results
            self.hub.store_data('dan2_results', {
                'trade_signals': trade_signals
            })
            logger.info("Dan2: Results stored in hub.")
        except Exception as e:
            logger.error(f"Dan2: Execution failed: {e}")
            raise