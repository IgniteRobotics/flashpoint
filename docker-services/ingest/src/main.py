import subprocess
import time
from ping3 import ping

botIP = "10.68.29.2"
botUser = "lvuser"
botPass = ""
teleDir = "/home/lvuser/logs"
allDir = teleDir+"/*"
botHostname = botUser+"@"+botIP
watchdogDelay = 5
afterFoundDelay = 300

def check_ip_alive(ip_address):
    try:
        response = ping(ip_address, timeout=1)
        return response is not None
    except Exception:
        return False

def retrieveLogs():
	listFiles = [
		"ssh", botHostname, # login
		"-o StrictHostKeyChecking=no", # no "check fingerprint" message/error
		"-o UserKnownHostsFile=/dev/null", # don't save fingerprint
		"ls", # list all files
		"-1t", # one per line, in time order (newest first)
		teleDir, # directory
	]

	fullCommand = ""
	for str in listFiles:
		fullCommand = fullCommand + str + " "
	#print("Lv command: "+fullCommand)
	lsRes = subprocess.run(listFiles, capture_output=True).stdout.decode()
	#print(f"Ls logs: {lsRes}")
	#print("\nEnd of logs")
	splitLsRes = lsRes.splitlines()
	for line in splitLsRes:
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
