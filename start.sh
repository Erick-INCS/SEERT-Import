mkdir -p data
spark-shell --driver-class-path drivers/mssql-jdbc-9.2.1.jre11.jar,drivers/jaybird-full-3.0.9.jar --jars drivers/mssql-jdbc-9.2.1.jre11.jar,drivers/jaybird-full-3.0.9.jar
