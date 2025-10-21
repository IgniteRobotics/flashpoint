import subprocess
import sys
import time
import re

drive_dir = "/home/ignite/GoogleDrive/"
flashpoint_dir = "/home/ignite/workspace/flashpoint/"

compID_regex = r"[a-zA-Z][a-zA-Z][a-zA-Z][a-zA-Z][a-zA-Z]?[0-9]?_" # matches for any 4/5 letters, optional number, and underscore after

invalids = [
	"FRC", "TBD", "FF", "rio"
]

def backupLogs():
	list_files = [
		"ls", "-1", # list all files one per line
		flashpoint_dir+"telemetry/"
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
			"cp", telemetry_dir+fileName, drive_teledir # move file to drive_directory
		]
		moveCMD = subprocess.run(move_file, capture_output=True)
		moveRes = moveCMD.stdout.decode()
		moveErr = moveCMD.stdout.decode()
		if moveRes != "":
			print(repr(moveRes)) # repr to force printing non-printing chars, such as newlines

def backupDB():
	print("Copying robot.db file...")
	drive_db_dir = drive_dir+"Programming/Telemetry/LandingZone/dbbackups/"
	
	LT = time.localtime()
	strDate = str(LT.tm_year)+str(LT.tm_mon)+str(LT.tm_mday)
	strTime = str(LT.tm_hour)+str(LT.tm_min)+str(LT.tm_sec)
	#print(strDate+"_"+strTime)

	fullNewFilePath = drive_db_dir+"backup_"+strDate+"_"+strTime+".db"
	
	copy_file = [
		"cp", flashpoint_dir+"db/robot.db", # copy the robot db file
		fullNewFilePath
	]
	
	copyCMD = subprocess.run(copy_file, capture_output=True)
	copyRes = copyCMD.stdout.decode()
	copyErr = copyCMD.stderr.decode()
	
	if copyRes != "":
		print(copyRes)
	if copyErr != "":
		print(copyErr)

def main():
	print("Starting...")
	if len(sys.argv) > 1:
		if "--only-db" in sys.argv:
			backupDB()
			return
		elif "--only-logs" in sys.argv:
			backupLogs()
			return
		else:
			backupLogs()
			backupDB()
	elif len(sys.argv) == 2:
		print("Usage: drive-backup.py [OPTIONS]\n")
		print("Options:\n")
		print("--db                  backup database (robot.db) file")
		return

if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		print("\nMonitoring stopped by user")