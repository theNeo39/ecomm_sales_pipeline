spark-submit \
--master local[*] \
--driver-memory 2g
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 \
src/main/python/initialize.py dev
