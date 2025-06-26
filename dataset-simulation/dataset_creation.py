import subprocess
import time

startup_scripts = ["station_info_data.py",
                   "train_master_data.py",
                   "train_schedule_time_data.py"
                   ]


print("Starting startup scripts...\n")

for script in startup_scripts:
    try:
        subprocess.run(["python", script], check=True)
        print(f"Successfully executed {script}")
    except subprocess.CalledProcessError as e:
        print(f"Error executing {script}: {e}")

max_runs = 120

for i in range(max_runs):
    try:
        subprocess.run(["python", "train_live_status.py"], check=True)
        print(f"Run {i+1}/{max_runs} completed successfully.")
        time.sleep(30)
    except subprocess.CalledProcessError as e:
        print(f"Error during run {i+1}: {e}")


print("All scheduled runs complete.")