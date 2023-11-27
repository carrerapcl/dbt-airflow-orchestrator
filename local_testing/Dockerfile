FROM apache/airflow:2.4.0-python3.8

USER root

# If SSL certificates are required, users can add their certificates here.
# For example:
# COPY path_to_your_cert.pem /usr/local/share/ca-certificates/your_cert.crt
# RUN update-ca-certificates

USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

COPY airflow.cfg /opt/airflow/airflow.cfg
