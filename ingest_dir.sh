for file in data/rio_logs/2025/*.wpilog
do
  python ingest_rio_log.py "$file" db/robot.db 2025
done
