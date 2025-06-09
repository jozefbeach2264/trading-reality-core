import json
import requests
import logging
from time import sleep

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def send_signal_to_executor(signal):
    """
    Write signal to signal.json and POST to Project 2.
    """
    # Write to signal.json
    with open("signal.json", "w") as f:
        json.dump(signal, f)
        logger.info(f"Signal written to signal.json: {signal}")
    
    # POST to Project 2
    url = "http://<project2-replit-url>/receive_signal"  # TBD: Replace with Project 2 URL
    retries = 3
    for attempt in range(retries):
        try:
            response = requests.post(url, json=signal, timeout=5)
            response.raise_for_status()
            logger.info(f"Signal sent to Project 2: {response.json()}")
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Retry {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                sleep(5)
    raise Exception("Failed to send signal after retries")