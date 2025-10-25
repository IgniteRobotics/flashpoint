import os
import shutil
def manage_imports():
    print("Managing imports")
    import_list = os.listdir("./imported_files")
    
    import_system_list = []
    import_drive_list = []
    import_rio_list = []
    
    for file in import_list:
        pos = file.rfind(".")
        if file[pos:] is "wpilog":
            import_system_list.append(file)
            shutil.copy("./data/imported_files/" + file, "./data/system_logs")
        elif file[pos:] is "hoot":
            if "rio" in file:
                import_rio_list.append(file)
                shutil.copy("./data/imported_files/" + file, "./data/rio_device_logs")
            else:
                import_drive_list.append(file)
                shutil.copy("./data/imported_files/" + file, "./data/drive_device_logs")
        else:
            print("Unrecognized filetype is in import directory!")
        os.remove("./imported_files/" + file)
    
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
    manage_imports()