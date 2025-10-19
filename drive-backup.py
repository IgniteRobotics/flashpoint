import subprocess
import sys
import re

drive_dir = "/home/ignite/GoogleDrive/"
telemetry_dir = "/home/ignite/workspace/flashpoint/telemetry/"

compID_regex = r"[a-zA-Z]+[0-9]_"

def main():
	list_files = [
		"ls", "-1", # list all files one per line
		telemetry_dir
	]
	listRes = subprocess.run(list_files, capture_output=True).stdout.decode()

	for fileName in listRes.splitlines():
		print("File: "+fileName, end=" ")
		compID_match = re.search(compID_regex, fileName)
		drive_teledir = ""
		if compID_match:
			compID = ""
			for i in range(compID_match.start(), compID_match.end()-1):
				compID = compID+fileName[i]
			drive_teledir = drive_dir+"Programming/Telemetry/LandingZone/"+compID
			print("Comp id: "+compID)
		else:
			drive_teledir = drive_dir+"Programming/Telemetry/LandingZone/nocomp"
			print("Comp id: none")
		make_folder = [
			"mkdir", "-p", # make folder, all parent directories, and don't complain if existing
			drive_teledir,
		]
		mkdirRes = subprocess.run(make_folder, capture_output=True).stdout.decode()
		if mkdirRes != "":
			print(repr(mkdirRes)) # repr to force printing non-printing chars, such as newlines

		move_file = [
			"cp",# telemetry_dir+fileName, drive_teledir # move file to drive_directory
		]
		moveCMD = subprocess.run(move_file, capture_output=True)
		moveRes = moveCMD.stdout.decode()
		moveErr = moveCMD.stdout.decode()
		if moveRes != "":
			print(repr(moveRes)) # repr to force printing non-printing chars, such as newlines

if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		print("\nMonitoring stopped by user")