for file in data/device_logs/*.hoot
do
  python ingest_device_log.py "$file" 
done
