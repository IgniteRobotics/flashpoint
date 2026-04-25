from pathlib import Path
import subprocess
import time
import sys
import os
import re

matchlog_regex = r"[A-Z][A-Z][A-Z][A-Z][A-Z]?[0-9]?_[EQP][0-9][0-9]?"
telemetryDir = "./telemetry/"

def organize(path):
	print("Organizing files...")
	files = [f for f in path.rglob('*') if f.is_file()]
	for file in files:
		if file.name.startswith("."):
			continue
		print("- "+file.name)
		match = re.search(matchlog_regex, file.name)
		if match is None:
			matchFolder = Path(telemetryDir+"nomatch")
		else:
			matchFolder = Path(telemetryDir+match[0])
		if not matchFolder.exists(): matchFolder.mkdir()
		if Path(telemetryDir+file.name).is_file():
			os.system("mv ./telemetry/"+file.name+" ./"+matchFolder.__str__())
	print("Finished organizing files.")

def main():
	while True:

		if Path(telemetryDir).rglob("*").__sizeof__() > 0:
			path = Path(telemetryDir)
			organize(path)
			os.system("python3 ingest_dir.py")
			time.sleep(10000) # 10s
		time.sleep(50)

if __name__ == "__main__":
	print("Starting importer...")
	try:
		main()
	except KeyboardInterrupt:
		print("\nMonitoring stopped by user")