import sys
import signal
import logging
from src.config import config
from src.consumer import StreamingProcessor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("stream-processor-main")

def main():
    logger.info("Initializing CDC & Streaming Enrichment Processor...")
    processor = StreamingProcessor(config)

    def handle_signal(sig, frame):
        logger.info(f"Signal {sig} received, stopping processor...")
        processor.running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info("Running processor loop...")
    processor.run()

if __name__ == "__main__":
    main()
