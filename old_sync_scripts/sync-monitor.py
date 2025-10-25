import subprocess
import time
from ping3 import ping  # You'll need to install this: pip install ping3

def check_ip_alive(ip_address):
    try:
        response = ping(ip_address, timeout=1)
        return response is not None
    except Exception:
        return False

def run_rsync(source_path, destination_path):
    try:
        rsync_command = [
            "rsync",
            "-avz",  # archive mode, verbose, compress
            "--delete",  # delete extraneous files from destination
            source_path,
            destination_path
        ]
        subprocess.run(rsync_command, check=True)
        print("Rsync completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Rsync failed with error: {e}")
        return False

def main():
    # Configuration
    target_ip = "192.168.1.100"  # Replace with your target IP
    source_path = "/path/to/source/"  # Replace with your source path
    destination_path = "user@192.168.1.100:/path/to/destination/"  # Replace with your destination
    check_interval = 60  # Check every 60 seconds

    print(f"Starting to monitor for IP: {target_ip}")
    
    while True:
        if check_ip_alive(target_ip):
            print(f"Device at {target_ip} is online")
            print("Starting rsync operation...")
            run_rsync(source_path, destination_path)
            # Wait longer after a successful sync to avoid constant syncing
            time.sleep(300)  # Wait 5 minutes before next check
        else:
            print(f"Device at {target_ip} is not reachable")
            time.sleep(check_interval)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user")
