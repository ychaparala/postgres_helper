# postgres_helper

This utility call native postgres C bindings to call COPY utility to load CSV data into postgresdb

Test
---------

Example command to load csv to hyperstage

`python postgres_helper.py --in-avro-file-location /archive/Answers.avro --postgres-db stackoverflow --postgres-table answers --postgres-hostname hostname --postgres-username username --postgres-jceks-location ./postgres.jceks --postgres-password-alias
secret_alias`

Example command to dump data from hyperstage to csv

`python postgres_helper.py --out-csv-file-location /archive/Answers.csv --out-csv-filter-condition "create_date='2022-01-04'" --postgres-db stackoverflow --postgres-table answers --postgres-hostname hostname --postgres-username username --postgres-jceks-location ./postgres.jceks --postgres-password-alias
secret_alias`


Example command to load csv to Oracle

`python oralce_helper.py --in-avro-file-location /archive/Answers.avro --oracle-hostname hostname --oracle-sid test --oracle-username username --oracle-jceks-location /path_to_jceks/oracle.jceks --oracle-password-alias password.alias --oracle-db oracle_db --oracle-table oracle_table`

Example command to dump data from Oracle to csv

`python oralce_helper.py --out-csv-file-location /archive/Answers.csv --out-csv-filter-condition "create_date='2022-01-04'" --oracle-hostname hostname --oracle-sid test --oracle-username username --oracle-jceks-location /path_to_jceks/oracle.jceks --oracle-password-alias password.alias --oracle-db oracle_db --oracle-table oracle_table`