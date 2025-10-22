for file in data/drive_device_logs/*.hoot
do
  python ingest_device_log.py "$file" 
for file in data/rio_device_logs/*.hoot
do
  python ingest_device_log.py "$file"
done
