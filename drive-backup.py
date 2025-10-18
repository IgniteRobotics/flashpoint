import subprocess
import sys
import re

drive_dir = "/home/ignite/GoogleDrive/"
telemetry_dir = "./telemetry/"

compID_regex = r"GA[a-zA-Z][a-zA-Z][a-zA-Z]\_"

def main():
	list_files = [
		"ls", "-1", # list all files one per line
		telemetry_dir
	]
	listRes = subprocess.run(list_files, capture_output=True).stdout.decode()

	for fileName in listRes.splitlines():
		compID = fileName
		drive_teledir = drive_dir+"Programming/Telemetry/LandingZone/"
		move_file = [
			"mv", fileName, drive_teledir # move file to drive_directory
		]

if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		print("\nMonitoring stopped by user")