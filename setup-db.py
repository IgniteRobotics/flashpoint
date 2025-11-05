from ingest_library import setup_db
c = setup_db("db/robot.db")
c.close()