# It contains Miniconda3 from dockerhub
FROM continuumio/miniconda3

ENV BASE_DIR                /opt
ENV NOTEBOOKS_HOME          ${BASE_DIR}/notebooks
ENV SPARK_HOME              ${BASE_DIR}/spark
ENV PYSPARK_DRIVER_PYTHON   ipython
ENV PATH                    ${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}

# Python Packages
RUN conda update -n base conda && \
    conda install numpy pandas matplotlib scikit-learn jupyter ipython pyspark=2.3.0 -y --quiet && \
    conda clean -tipsy && \
    mkdir -p ${NOTEBOOKS_HOME}

# OpenJDK 8
RUN apt-get update --fix-missing && \
    apt-get install -y openjdk-8-jdk-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Spark 2.3.0
RUN curl -s https://archive.apache.org/dist/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz | tar xz -C /tmp && \
    mv /tmp/spark-2.3.0-bin-hadoop2.7 ${SPARK_HOME}

# Google Cloud Storage Connector
RUN wget -q https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar -P ${SPARK_HOME}/jars

WORKDIR ${BASE_DIR}/work