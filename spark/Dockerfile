FROM bitnami/spark:latest

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir \
        pyspark==4.0.0 \
        boto3 \
        kafka-python \
        pandas \
        pyarrow \
        delta-spark