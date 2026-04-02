FROM python:3.14.1-bookworm
# was python:3.12.3-slim-bullseye

RUN apt-get update && apt-get install -y --no-install-recommends sshpass ssh
RUN rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --upgrade pip

COPY puller-requirements.txt .
RUN pip install --no-cache-dir -r puller-requirements.txt

COPY . .

#RUN ls -lag  && sleep 15

RUN mkdir /app/telemetry/
RUN mkdir /app/converted_data/
RUN mkdir /app/db/

EXPOSE 22

RUN python3 setup-db.py

CMD ["python", "docker-services/puller/src/main.py"]