import logging
import logging_loki
import time

# Configure the Loki logging handler
logging_loki.emitter.LokiEmitter.level_tag = "level"
handler = logging_loki.LokiHandler(
    url="http://localhost:3100/loki/api/v1/push",
    tags={"application": "test-app"},
    version="1",
)

# Configure the Python logger
logger = logging.getLogger("loki-logger")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Optional: Add console handler to see logs in terminal
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)

def main():
    while True:
        try:
            # Example logs at different levels
            logger.info("This is an info message", extra={"tags": {"service": "user-service"}})
            logger.warning("This is a warning message", extra={"tags": {"service": "user-service"}})
            logger.error("This is an error message", extra={"tags": {"service": "user-service"}})
            
            # Wait for 5 seconds before sending next logs
            time.sleep(5)
            
        except Exception as e:
            logger.error(f"Error occurred: {str(e)}", extra={"tags": {"service": "user-service"}})
            break

if __name__ == "__main__":
    main()
