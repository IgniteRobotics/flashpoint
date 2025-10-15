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

SPACESEPARATOR = " "


def check_ip_alive(ip_address):
    try:
        response = ping(ip_address, timeout=1)
        return response is not None
    except Exception:
        return False
	
passPrefix = [
	"sshpass", "-p", botPass,
]

sshPrefix = passPrefix + [
	"ssh", botHostname, # login
	"-o StrictHostKeyChecking=no", # no "check fingerprint" message/error
	"-o UserKnownHostsFile=/dev/null", # don't save fingerprint
]

getDate = [
	"date", # get date
	"+%Y%m%d" # format into YYYYMMDD
]

def retrieveLogs():
	date = int(subprocess.run(getDate, capture_output=True).stdout.decode())

	listFiles = sshPrefix + [
		"ls", # list all files
		"-1t", # one per line, in time order (newest first)
		teleDir, # directory
	]
	grepFilter = listFiles + ["|",
		"find", # find all files
		"|", # and then
		"grep", f"*{date-timeSlot}" # filter to only files within specified days
	]

	lsRes = ""
	lsErr = ""
	if timeSlot == 0:
		lsCmd = subprocess.run(listFiles, capture_output=True)
		lsRes = lsCmd.stdout.decode()
		lsErr = lsCmd.stderr.decode()
	else:
		lsCmd = subprocess.run(grepFilter, capture_output=True)
		lsRes = lsCmd.stdout.decode()
		lsErr = lsCmd.stderr.decode()


	if len(lsErr.splitlines()) > 1:
		print("Something failed: "+lsErr)
		print("Logs: "+lsRes)
		return

	if len(lsRes.splitlines()) == 0 and timeSlot != 0:
		print(f"\nFailed to find any logs within {timeSlot} days")
		return
	elif len(lsRes.splitlines()) == 0 and timeSlot == 0:
		print(f"There appears to be no logs in this directory on the host: {teleDir}")
		return

	for line in lsRes.splitlines():
		print(line)
		scpSuccessFlag = False

		scpCommand = passPrefix + [
			"scp",
			"-o StrictHostKeyChecking=no", # no "check fingerprint" message/error
			"-o UserKnownHostsFile=/dev/null", # don't save fingerprint
			botHostname+":"+teleDir+"/"+line, "/app/telemetry/", # take one user@ip:/path/to/logs/ file and store in /app/telemetry/
		]

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

	# start ingest
	print("Starting Ingest on files in ./telemetry")
	ingestCMD = subprocess.run(["python3", "./ingest_dir.sh"], capture_output=True)
	ingestRes = ingestCMD.stdout.decode()
	ingestErr = ingestCMD.stderr.decode()
	print(ingestRes)
	print(ingestErr)

	"""for fileName in lsRes.splitlines():
		rmCMD = subprocess.run(["rm", "./telemetry/"+fileName], capture_output=True)
		rmRes = rmCMD.stdout.decode()
		rmErr = rmCMD.stderr.decode()
		print(rmRes)
		print(rmErr)"""

def main():
	global timeSlot
	if len(sys.argv) > 1:
		if "--time" in sys.argv and len(sys.argv) > 2:
			timeSlot = int(sys.argv[2])
		if "--force" in sys.argv:
			retrieveLogs()
			return
	elif len(sys.argv) == 2:
		print("Usage: main.py [OPTIONS]\n")
		print("Options:\n")
		print("--time <within_days>     filter to files within X days")
		print("--force                  force retrieval without pinging, will run only once")
		return
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
