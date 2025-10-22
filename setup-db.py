from ingest_file import setup_db
c = setup_db("db/robot.db")
c.close()