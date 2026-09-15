# Extends the official slim Airflow image with what our tasks actually
# need: a JDK (for PySpark) and the project's own Python dependencies.
# Project code and secrets are volume-mounted at runtime, not baked in
# here - see docker-compose.yaml.

FROM apache/airflow:2.10.4-python3.12

USER root

# PySpark needs a JDK. Install it, then symlink to a fixed path so
# JAVA_HOME is stable regardless of the exact arch-specific folder name
# apt installs it under (differs between amd64 and arm64 hosts).
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17-openjdk-current \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-current
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# So `python /opt/airflow/project/src/...` can find sibling modules if
# the scripts ever import from each other across folders.
ENV PYTHONPATH="/opt/airflow/project"