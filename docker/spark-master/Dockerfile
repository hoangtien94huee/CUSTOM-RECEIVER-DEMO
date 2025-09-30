FROM apache/spark:v3.3.0

# Set the environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Expose the Spark Master UI and RPC port
EXPOSE 8080 7077

# Set the entrypoint to start the Spark Master
ENTRYPOINT ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]