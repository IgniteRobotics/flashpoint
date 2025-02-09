for file in data/*.wpilog
do
  python ingest_file.py "$file"
done
