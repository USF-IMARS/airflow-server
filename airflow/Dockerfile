FROM apache/airflow:latest

USER root
# =============================================================

# === install git + pip
RUN apt-get update && \
    apt-get install --yes git python3-pip

USER airflow
# ===========================================================

# === install python dependencies from requirements.txt
ADD requirements.txt .

RUN pip install --upgrade pip
RUN pip install --upgrade apache-airflow==${AIRFLOW_VERSION} 
RUN pip install --upgrade --force-reinstall --no-cache-dir -r requirements.txt