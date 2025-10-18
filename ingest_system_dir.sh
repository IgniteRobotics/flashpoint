for file in data/system_logs/2025/*.wpilog
do
  python ingest_system_log.py "$file" db/robot.db 2025
done
