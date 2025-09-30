FROM apache/spark:v3.3.0

# Set the environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Set the working directory
WORKDIR /app

# Expose the Spark Worker UI port
EXPOSE 8081

# Set the entry point for the Spark Worker
ENTRYPOINT ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077"]