FROM python:3.12.3-slim-bullseye

#RUN apt-get update && apt-get install -y --no-install-recommends iputils-ping sshpass ssh 
#RUN rm -rf /var/lib/apt/lists/*
    
WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r docker-services/dataviz/requirements.txt
RUN pip install --no-cache-dir -r viz-requirements.txt

RUN mkdir /app/db/
RUN mkdir /app/converted_data/
RUN mkdir /app/telemetry/

EXPOSE 8501

CMD ["sh", "docker-services/dataviz/src/main.sh"]