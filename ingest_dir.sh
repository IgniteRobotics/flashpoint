for file in telemetry/*.wpilog
do
  python3 ingest_file.py "$file" db/robot.db
done
