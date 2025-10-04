for file in data/hoot_logs/*.hoot
do
  python ingest_hoot_log.py "$file" 
done
