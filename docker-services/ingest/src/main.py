import subprocess
import time
from ping3 import ping

botIP = "10.68.29.2"
botUser = "lvuser"
botPass = ""
teleDir = "/home/lvuser/logs"
allDir = teleDir+"/*"
botHostname = botUser+"@"+botIP

def check_ip_alive(ip_address):
    try:
        response = ping(ip_address, timeout=1)
        return response is not None
    except Exception:
        return False

def retrieveLogs():
	scpSuccessFlag = False

	scpCommand = [
		#"sshpass", "-p", botPass, # password
   		"scp",
		"-o StrictHostKeyChecking=no", # no "check fingerprint" message/error
		"-o UserKnownHostsFile=/dev/null", # don't save fingerprint
		botHostname+":"+allDir, "/app/telemetry/", # take all user@ip:/path/to/logs/ files and store in /app/telemetry/
	]
	removeCommand = [
		#"sshpass", "-p", botPass, # password
		"ssh", botHostname, # login
		"rm", "-f", f"'{allDir}'", # delete all copied files
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
			time.sleep(60)
if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		print("\nMonitoring stopped by user")
