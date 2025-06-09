# hub.py
import pickle
import os
import logging

logger = logging.getLogger(__name__)

class Hub:
    def __init__(self):
        self.data_store = {}
        self.storage_path = "dan_hub_storage.pkl"

    def store_data(self, key, data):
        """Store data in memory and persist to disk."""
        try:
            self.data_store[key] = data
            with open(self.storage_path, 'wb') as f:
                pickle.dump(self.data_store, f)
            logger.info(f"Hub: Stored data for key {key}")
        except Exception as e:
            logger.error(f"Hub: Error storing data: {e}")
            raise

    def get_data(self, key):
        """Retrieve data from memory or disk."""
        try:
            if key in self.data_store:
                return self.data_store[key]
            if os.path.exists(self.storage_path):
                with open(self.storage_path, 'rb') as f:
                    self.data_store = pickle.load(f)
                return self.data_store.get(key, None)
            logger.warning(f"Hub: No data found for key {key}")
            return None
        except Exception as e:
            logger.error(f"Hub: Error retrieving data: {e}")
            raise

    def has_data(self, key):
        """Check if data exists for a key."""
        return key in self.data_store or os.path.exists(self.storage_path)