for file in data/rio_logs/*.wpilog
do
  python ingest_rio_log.py "$file" db/robot.db
done
