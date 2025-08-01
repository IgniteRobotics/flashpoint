import csv
from datalog import DataLogReader
import gzip
from datetime import datetime
import mmap
import sys
import os

def csv_convert(file_name):
     with open(file_name, "r") as f:
        #starts up the wpilog reader
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        reader = DataLogReader(mm)
        if not reader:
            print("not a log file", file=sys.stderr)
            sys.exit(1)
            
        #hashmap
        entries = {}
        
        #argument is the wpilog you want it to convert
        input_log = sys.argv[1]
        
        #creates and opens a csv file with the same name as your wpilog file (except the suffix)
        pos = input_log.rfind(".")
        output_csv = input_log[:pos] + ".csv"        
        with open(output_csv, 'w+', newline='') as csvfile:
            #prepares the writer
            csv_writer = csv.writer(csvfile, delimiter=',',
                                    quotechar='|', quoting=csv.QUOTE_MINIMAL)
            #wpilogs have asynchronous writing
            #they will have start and end records and records with actual data in between all written together at the same time
            #these start and end periods each represent a chunk a time, so you will get multiple start records under the same name in the same log file
            #this throws all the unreadable data into a slightly more readable csv file
            #really you can get all the necessary info from the records by ignoring the starts and ends
            #however the hashmap serves as a safety net to prevent data that does not have a matching "start" from being added
            for record in reader:
                #timestamps are not useful if they are in nanoseconds
                timestamp = record.timestamp / 1000000
                
                #add entry into hashmap if a start record is read in (data coming for that entry supposedly)
                if record.isStart():
                    try:
                        data = record.getStartData()
                        entries[data.entry] = data
                    except TypeError as e:
                        print("Start(INVALID)")
                        
                #remove entry if there is a finish record read in (no more data supposedly)
                elif record.isFinish():
                    try:
                        entry = record.getFinishEntry()
                        print(f"Finish({entry}) [{timestamp}]")
                        if entry not in entries:
                            print("...ID not found")
                        else:
                            del entries[entry]
                    except TypeError as e:
                        print("Finish(INVALID)")
                        
                #metadata can be printed to console but otherwise is not used
                elif record.isSetMetadata():
                    try:
                        data = record.getSetMetadataData()
                        print(f"SetMetadata({data.entry}, '{data.metadata}') [{timestamp}]")
                        if data.entry not in entries:
                            print("...ID not found")
                    except TypeError as e:
                        print("SetMetadata(INVALID)")
                        
                #control records are completely thrown out
                elif record.isControl():
                        print("Unrecognized control record")
                        
                #if it is a real data record
                else:
                    #only use it if it has a recognizable "start" record tied to it
                    entry = entries.get(record.entry)
                    if entry is None:
                        print("<ID not found>")
                        continue
                    try:
                        # handle systemTime specially
                        if entry.name == "systemTime" and entry.type == "int64":
                            dt = datetime.fromtimestamp(record.getInteger() / 1000000)
                            continue

                        #all other different data types are handled
                        if entry.type == "double":
                            value = record.getDouble()
                            csv_writer.writerow([entry.name, entry.type, value, record.timestamp])
                        elif entry.type == "int64":
                            value = record.getInteger()
                            csv_writer.writerow([entry.name, entry.type, value, record.timestamp])
                        elif entry.type in ("string", "json"):
                            value = record.getString()
                            value = value.replace("\r", " ")
                            value = value.replace("\n", " ")
                            csv_writer.writerow([entry.name, entry.type, value, record.timestamp])
                        elif entry.type == "msgpack":
                            value = record.getMsgPack()
                            csv_writer.writerow([entry.name, entry.type, value, record.timestamp])
                        elif entry.type == "boolean":
                            value = record.getBoolean()
                            csv_writer.writerow([entry.name, entry.type, value, record.timestamp])
                        elif entry.type == "boolean[]":
                            arr = record.getBooleanArray()
                            csv_writer.writerow([entry.name, entry.type, arr, record.timestamp])
                        elif entry.type == "double[]":
                            arr = record.getDoubleArray()
                            csv_writer.writerow([entry.name, entry.type, arr, record.timestamp])
                        elif entry.type == "float[]":
                            arr = record.getFloatArray()
                            csv_writer.writerow([entry.name, entry.type, arr, record.timestamp])
                        elif entry.type == "int64[]":
                            arr = record.getIntegerArray()
                            csv_writer.writerow([entry.name, entry.type, arr, record.timestamp])
                        elif entry.type == "string[]":
                            arr = record.getStringArray()
                            csv_writer.writerow([entry.name, entry.type, arr, record.timestamp])
                    except TypeError as e:
                        print("  invalid")
                    except UnicodeEncodeError:
                        print(arr)
                        print("   UnicodeEncodeError")

        #compresses the output .csv file into a .gz file
        f_in = open(output_csv)
        f_out = gzip.open(output_csv[:-3]+"gz", 'wt')
        f_out.writelines(f_in)
        f_out.close()
        f_in.close()
        os.remove(output_csv)
        print("--Complete--")

#script that actually runs when 
if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: csv_converter.py <file>", file=sys.stderr)
        sys.exit(1)

    csv_convert(sys.argv[1])
