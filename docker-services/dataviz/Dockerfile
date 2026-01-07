FROM python:3.12.3-slim-bullseye

#RUN apt-get update && apt-get install -y --no-install-recommends iputils-ping sshpass ssh 
RUN apt-get update && apt-get install -y --no-install-recommends libglib2.0-0 libnss3 libdbus-1-3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libgtk-3-0 libgbm1 libasound2
#RUN rm -rf /var/lib/apt/lists/*
    
WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r docker-services/dataviz/requirements.txt
RUN pip install --no-cache-dir -r viz-requirements.txt

RUN mkdir /app/db/
RUN mkdir /app/converted_data/
RUN mkdir /app/telemetry/

EXPOSE 8501

CMD ["sh", "docker-services/dataviz/src/main.sh", "--server.headless true"]