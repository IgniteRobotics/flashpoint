for file in data/system_logs/*.wpilog
do
  python ingest_system_log.py "$file" db/robot.db 2025
done
