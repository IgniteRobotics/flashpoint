import subprocess
import time
from ping3 import ping

botIP = "10.68.29.2"
botUser = "lvuser"
botPass = ""
teleDir = "/home/lvuser/logs"
allDir = teleDir+"/*"
botHostname = botUser+"@"+botIP
watchdogDelay = 15

def check_ip_alive(ip_address):
    try:
        response = ping(ip_address, timeout=1)
        return response is not None
    except Exception:
        return False

def retrieveLogs():
	listFiles = [
		"ssh", botHostname, # login
		"ls", # list all files
		"-1t", # one per line, in time order (newest first)
		"/app/telemetry", # directory
	]
	lsRes = subprocess.run(listFiles, capture_output=True).stdout.decode()

	for line in lsRes:
		scpSuccessFlag = False
		scpCommand = [
			#"sshpass", "-p", botPass, # password
			"scp",
			"-o StrictHostKeyChecking=no", # no "check fingerprint" message/error
			"-o UserKnownHostsFile=/dev/null", # don't save fingerprint
			botHostname+":"+teleDir+line, "/app/telemetry/", # take one user@ip:/path/to/logs/ file and store in /app/telemetry/
		]
		removeCommand = [
			#"sshpass", "-p", botPass, # password
			"ssh", botHostname, # login
			"rm", "-f", f"'{line}'", # delete copied file
		]

		res = subprocess.run(scpCommand, capture_output=True).stderr.decode()
		scpRes = ""
		if "No such file or directory" in res:
			scpRes = "Error: " + res + f"\n - {scpCommand}"
		else:
			scpRes = res
			scpSuccessFlag = True
		print("Retrieving logs:", scpRes)

		if scpSuccessFlag:
			removeRes = subprocess.run(removeCommand, capture_output=True).stderr.decode()
			finalRemoveRes = ""
			if "No such file or directory" in removeRes:
				finalRemoveRes = "Error: " + removeRes + f"\n - {removeCommand}"
			else:
				finalRemoveRes = removeRes
			print("Removing old logs:", finalRemoveRes)

		with open("scpLog.log", 'a') as scpLogFile:
			scpLogFile.write(res + "\n" + finalRemoveRes)

def main():
	print("Starting...")
	subprocess.run(["touch","scpLog.log"])
	while True:
		pingRes = check_ip_alive(botIP)
		if pingRes:
			print("Found machine:", botIP)
			retrieveLogs()
			time.sleep(300)
		else:
			print(f"Unable to ping address: {botIP}")
			time.sleep(watchdogDelay)
if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		print("\nMonitoring stopped by user")
