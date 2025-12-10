from src.utils import start_logger
import subprocess

logging = start_logger()

def run_step(script):
    logging.info(f"Running {script}...")
    subprocess.run(["python", script], check=True)
    logging.info("Done.\n")

if __name__ == "__main__":
    run_step("scripts/parse_raw_data.py")
    run_step("scripts/parse_raw_meta.py")
    run_step("scripts/transform_data.py")