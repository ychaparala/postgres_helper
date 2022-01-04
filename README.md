# postgres_helper

This utility call native postgres C bindings to call COPY utility to load CSV data into postgresdb

Test
---------

Example command to load csv to hyperstage

`python postgres_helper.py --in-csv-file-location /archive/Answers.csv --postgres-db stackoverflow --postgres-table answers --postgres-hostname hostname --postgres-username username --postgres-jceks-location ./postgres.jceks --postgres-password-alias
secret_alias`

Example command to dump data from hyperstage to csv

`python postgres_helper.py --out-csv-file-location /archive/Answers.csv --postgres-db stackoverflow --postgres-table answers --postgres-hostname hostname --postgres-username username --postgres-jceks-location ./postgres.jceks --postgres-password-alias
secret_alias`