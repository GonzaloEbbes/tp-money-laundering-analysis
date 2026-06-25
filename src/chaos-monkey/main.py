import os
import random
import time
import logging
import docker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MIN_WAIT = int(os.environ.get("MIN_WAIT", "10"))
MAX_WAIT = int(os.environ.get("MAX_WAIT", "30"))
DEAD_DURATION = int(os.environ.get("DEAD_DURATION", "10"))

MIN_KILL_COUNT = int(os.environ.get("MIN_KILL_COUNT", "1"))
MAX_KILL_COUNT = int(os.environ.get("MAX_KILL_COUNT", "1"))

WHITELIST_STR = os.environ.get("WHITELIST", "rabbitmq,gateway,client,chaos_monkey")
WHITELIST = [name.strip() for name in WHITELIST_STR.split(",") if name.strip()]
TEST_MODE = int(os.environ.get("TEST_MODE", "0"))

def get_worker_containers(client):
    containers = client.containers.list()
    workers = []
    for c in containers:
        is_protected = any(protected_name in c.name for protected_name in WHITELIST)
        if not is_protected:
            workers.append(c)
    return workers

def run():
    logging.info("Initializing Chaos Monkey resilience testing module.")
    logging.info("Test mode (auto-recovery): %s", bool(TEST_MODE))
    logging.info("Attack configuration: Terminating between %d and %d nodes per cycle.",
                 MIN_KILL_COUNT, MAX_KILL_COUNT)
    logging.info("Protected containers (Whitelist): %s", ", ".join(WHITELIST))

    try:
        client = docker.from_env()
    except Exception as e:
        logging.error("Failed to connect to Docker daemon. Reason: %s", e)
        return

    try:
        while True:
            workers = get_worker_containers(client)
            if not workers:
                logging.warning("No targetable worker containers found. Retrying in 5 seconds...")
                time.sleep(5)
                continue

            kill_count = random.randint(MIN_KILL_COUNT, MAX_KILL_COUNT)
            targets = random.sample(workers, min(kill_count, len(workers)))
            target_names = [t.name for t in targets]

            logging.info("Executing SIGKILL on %d containers: %s", 
                         len(targets), ", ".join(target_names))
            for target in targets:
                target.kill()

            if TEST_MODE:
                logging.info("Containers terminated. Waiting %d seconds before initiating recovery.",
                             DEAD_DURATION)
                time.sleep(DEAD_DURATION)
                logging.info("Initiating recovery sequence for containers: %s"
                             , ", ".join(target_names))
                for target in targets:
                    target.start()

            sleep_time = random.randint(MIN_WAIT, MAX_WAIT)
            logging.info("Cycle complete. Waiting for %d seconds before next attack.\n", sleep_time)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        logging.info("Chaos Monkey execution halted by user interrupt.")
    except Exception as e:
        logging.exception("An unexpected error occurred during execution: %s", e)

if __name__ == "__main__":
    run()
