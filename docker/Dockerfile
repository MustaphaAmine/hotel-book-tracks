ARG AIRFLOW_IMG="apache/airflow:2.2.4-python3.9"
FROM ${AIRFLOW_IMG}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
          vim \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /data \
    && chown airflow /data

RUN pip install --user --no-cache-dir \
    apache-airflow-providers-google==1.0.0
    # apache-airflow-providers-google==2.2.0

USER airflow