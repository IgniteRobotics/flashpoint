import sys
import os
from pathlib import Path
from ingest_match_logs import *
from csv_converter import *
import re

def ingest_dir_main():
    #os.makedirs("./converted-data/converted_drive_device_logs")
    #os.makedirs("./converted-data/converted_rio_device_logs")
    #os.makedirs("./converted-data/converted_system_logs")

    matchlogs = {}
    matchlog_regex = r"[A-Z][A-Z][A-Z][A-Z][A-Z]?_[EQ][0-9][0-9]?"

    telemetry_wpilogs = Path("./telemetry/").glob("**/*.wpilog")
    for file in telemetry_wpilogs:
      if file.name.startswith("."):
         continue
      match = re.search(matchlog_regex, file.name)
      if match:
        matchlogs[file.name] = match.group(0)
      else:
        matchlogs[file.name] = None

    for system_wpilog in matchlogs:
      matchid = matchlogs[system_wpilog]
      if matchid: # logs that have a match assigned
        print("File has a match: "+str(matchid))
        matchpath = "./telemetry/"+matchid
        # get other match logs
        matchid_logs = []

        telemetry_hoots = Path("./telemetry/").glob("**/*.hoot") # list all hootlogs
        for file in telemetry_hoots:
          if matchid in file.name:
            matchid_logs.append(file.name) # add log to list of logs from specified match
        
        drivetrain_hoot = ""
        rio_hoot = ""

        for file in matchid_logs: # assign riolog and drivetrainlog SIDE EFFECT: when multiple have rio (or dont) in it's name, it will pick the newest
          if "rio" in file:
            rio_hoot = file
          else:
            drivetrain_hoot = file
        
        if drivetrain_hoot == "" or rio_hoot == "":
          print("Could not find either RIO hootlog or drivetrain hootlog for file: "+system_wpilog)
          continue
        
        ingest_match_logs(f"{matchpath}/{system_wpilog}", f"{matchpath}/{drivetrain_hoot}", f"{matchpath}/{rio_hoot}", "db/robot.db")
      else:
        command = ["python3", "ingest_system_log.py", "./telemetry/"+system_wpilog, "db/robot.db", "2026"]

        ingestCMD = subprocess.run(command, capture_output=True)
        ingestRes = ingestCMD.stdout.decode()
        ingestErr = ingestCMD.stderr.decode()
        if ingestRes != "": print(ingestRes)
        if ingestErr != "": print(ingestErr)

if __name__ == "__main__":
  if len(sys.argv) > 1:
    print("This script does not use any arguments. Proceeding...")

  ingest_dir_main()