# postgres_helper

This utility call native postgres C bindings to call COPY utility to load CSV data into postgresdb

Test
---------

Example command to run the script

`python postgres_helper.py --csv-file-location /archive/Answers.csv --postgres-db stackoverflow --postgres-table answers --postgres-hostname hostname --postgres-username username --postgres-password password`