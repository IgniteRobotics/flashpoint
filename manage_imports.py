import os
import shutil
import sys


from ingest_match_logs import ingest_match_logs
def manage_imports():
    print("Managing imports")
    import_list = os.listdir("./imported_files")
   
    import_system_list = []
    import_drive_list = []
    import_rio_list = []
   
    os.makedirs("./data", exist_ok=True)
    os.makedirs("./data/system_logs", exist_ok = True)
    os.makedirs("./data/rio_device_logs", exist_ok = True)
    os.makedirs("./data/drive_device_logs", exist_ok = True)
   
    for file in import_list:
        pos = file.rfind(".")
        if file[pos:] == ".wpilog":
            import_system_list.append(file)
            shutil.copy("./imported_files/" + file, "./data/system_logs")
        elif file[pos:] == ".hoot":
            if "rio" in file:
                import_rio_list.append(file)
                shutil.copy("./imported_files/" + file, "./data/rio_device_logs")
            else:
                import_drive_list.append(file)
                shutil.copy("./imported_files/" + file, "./data/drive_device_logs")
        else:
            print("Unrecognized filetype is in import directory!")
        #os.remove("./imported_files/" + file)
   
    #TODO handle exceptions
    ingest_dict = {}
    for file in import_system_list:
        match_id = file[:file.rfind(".")].split("_")[-1]
        ingest_dict[match_id] = ["./data/system_logs/" + file]
    for file in import_drive_list:
        match_id = file.split("_")[1]
        ingest_dict[match_id].append("./data/drive_device_logs/"+file)
    for file in import_rio_list:
        match_id = file.split("_")[1]
        ingest_dict[match_id].append("./data/rio_device_logs/"+file)
       
    return ingest_dict
   
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <file>", file=sys.stderr)
        sys.exit(1)
    ingest_dict = manage_imports()
    for match in list(ingest_dict.values()):
        ingest_match_logs(match[0], match[1], match[2],sys.argv[1])