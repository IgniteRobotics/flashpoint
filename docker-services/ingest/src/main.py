import subprocess
import time
import sys
from ping3 import ping # type: ignore

botIP = "10.68.29.2"
botUser = "lvuser"
botPass = ""
teleDir = "/home/lvuser/logs"
allDir = teleDir+"/*"
botHostname = botUser+"@"+botIP
watchdogDelay = 5
afterFoundDelay = 300
timeSlot = 1

if len(sys.argv) > 2:
	if sys.argv[1] == "--time" and int(sys.argv[2]):
		timeSlot = int(sys.argv[2])
elif len(sys.argv) == 2:
	print("Usage: main.py [--time <within_days>]")

def check_ip_alive(ip_address):
    try:
        response = ping(ip_address, timeout=1)
        return response is not None
    except Exception:
        return False
	
sshPrefix = [
	"ssh", botHostname, # login
	"-o StrictHostKeyChecking=no", # no "check fingerprint" message/error
	"-o UserKnownHostsFile=/dev/null", # don't save fingerprint
]

def retrieveLogs():
	listFiles = [
		"ls", # list all files
		"-1t", # one per line, in time order (newest first)
		teleDir, # directory
	]
	grepFilter = [
		"find", # find all files
		teleDir, # directory
		"-type", "f", # only find files, exclude directories/symlinks
		"-mtime", timeSlot, # filter time for within one day
		"-printf", "%T@ %p\n", # print in created time format
		"|", # and then
		"sort", "-nr", # sort newest first
		"|", # and then
		"cut", "-d' '", "-f2-" # crop text to only filenames
	]

	spaceSeparator = " "
	fullCommand = spaceSeparator.join(listFiles)
	#print("Ls command: "+fullCommand)
	lsRes = ""
	if timeSlot != 0:
		lsRes = subprocess.run(sshPrefix + grepFilter, capture_output=True).stdout.decode()
	else:
		lsRes = subprocess.run(sshPrefix + listFiles, capture_output=True).stdout.decode()
	#print(f"Ls logs: {lsRes}")
	#print("\nEnd of logs")

	if len(lsRes.splitlines()) == 0:
		print("\nFailed to find any logs within 24 hours")
		return

	for line in lsRes.splitlines():
		print(line)
		scpSuccessFlag = False

		scpCommand = [
			"scp",
			"-o StrictHostKeyChecking=no", # no "check fingerprint" message/error
			"-o UserKnownHostsFile=/dev/null", # don't save fingerprint
			botHostname+":"+teleDir+"/"+line, "/app/telemetry/", # take one user@ip:/path/to/logs/ file and store in /app/telemetry/
		]

		fullScpCommand = ""
		for str in scpCommand:
			fullScpCommand = fullScpCommand + str + " "
		#print(fullScpCommand)

		removeCommand = [
			"ssh", botHostname, # login
			"-o StrictHostKeyChecking=no", # no "check fingerprint" message/error
			"-o UserKnownHostsFile=/dev/null", # don't save fingerprint
			"rm", "-f", f"'{line}'", # delete copied file
		]

		res = subprocess.run(scpCommand, capture_output=True).stderr.decode()
		scpRes = ""
		print("Downloading file: "+line)
		if "No such file or directory" in res:
			scpRes = "Error: " + res + f"\n - {scpCommand}"
		else:
			scpRes = res
			scpSuccessFlag = True
			print("Retrieving logs:", scpRes)
		
		finalRemoveRes = ""
		if scpSuccessFlag:
			removeRes = subprocess.run(removeCommand, capture_output=True).stderr.decode()
			if "No such file or directory" in removeRes:
				finalRemoveRes = "Error: " + removeRes + f"\n - {removeCommand}"
			elif removeRes is None:
				finalRemoveRes = "null"
			else:
				finalRemoveRes = removeRes
			print("Removing old logs:", finalRemoveRes)

def main():
	print("Starting...")
	while True:
		pingRes = check_ip_alive(botIP)
		if pingRes:
			print("Found machine:", botIP)
			retrieveLogs()
			time.sleep(afterFoundDelay)
		else:
			print(f"Unable to ping address: {botIP}")
			time.sleep(watchdogDelay)
if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		print("\nMonitoring stopped by user")
