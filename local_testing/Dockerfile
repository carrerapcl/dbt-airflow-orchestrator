FROM apache/airflow:2.7.1-python3.9

USER root

# Install Git
RUN apt-get update && \
    apt-get install -y git

# If SSL certificates are required, users can add their certificates here.
# For example:
# COPY path_to_your_cert.pem /usr/local/share/ca-certificates/your_cert.crt
# RUN update-ca-certificates

# Switch back to the airflow user
USER airflow

# Copy and install requirements
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# Copy the Airflow configuration
COPY airflow.cfg /opt/airflow/airflow.cfg
