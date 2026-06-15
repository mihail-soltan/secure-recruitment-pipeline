## generam Keystore pentru brokerul Kafka
keytool -genkey -keystore kafka.server.keystore.jks -validity 365 -storepass <parola> -keypass <parola> -dname "CN=kafka" -storetype pkcs12

# exportam certificatul public in format PEM 
keytool -exportcert -rfc -keystore kafka.server.keystore.jks -storepass <parola> -file ca-cert.pem

# generam truststore
keytool -importcert -alias ca-cert -file ca-cert.pem -keystore kafka.server.truststore.jks -storepass <parola> -noprompt

# adaugam un utilizator in kafka 
docker exec -it kafka kafka-configs --bootstrap-server localhost:9094 --alter --add-config 'SCRAM-SHA-256=[password=<parola>]' --entity-type users --entity-name <user>

# adaugam modelul mistral in containerul de ollama 
docker exec -it ollama ollama run mistral

# rulam jobul in spark master
docker exec -it spark-master bash

/opt/spark/bin/spark-submit \
  --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp/.ivy2" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-apps/spark_job.py