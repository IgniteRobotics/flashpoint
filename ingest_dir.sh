for file in telemetry/*.wpilog
do
  python ingest_file.py "$file" db/robot.db
done
