FROM python:3.12.3-slim-bullseye

RUN apt-get update && apt-get install -y --no-install-recommends sshpass ssh
RUN rm -rf /var/lib/apt/lists/*
    
WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r docker-services/ingest/requirements.txt

#RUN ls -lag  && sleep 15
RUN pip install --no-cache-dir -r ingest-requirements.txt

RUN mkdir /app/telemetry/
RUN mkdir /app/converted_data/
RUN mkdir /app/db/

EXPOSE 22

RUN python3 setup-db.py

CMD ["python", "docker-services/ingest/src/main.py"]