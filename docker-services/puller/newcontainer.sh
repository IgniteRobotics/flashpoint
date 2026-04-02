#!/bin/bash
docker create -it \
--volume /var/lib/docker/volumes/flashpoint-telemetry:/app/telemetry/ \
--name flashpoint-ingest flashpoint-ingest

##!/bin/bash
#docker create -it \ # keep shell open even when no activity
#--volume /var/lib/docker/volumes/flashpoint-telemetry:/app/telemetry/ \ # specify volume:mount-point
#--name flashpoint-ingest flashpoint-ingest