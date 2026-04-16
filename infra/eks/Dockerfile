FROM python:3.11

# Build with the repository root as the context, for example:
# docker build -f infra/eks/Dockerfile .
COPY ./etl /weatherapp-etl

# This makes sure that logs show up immediately instead of being buffered
ENV PYTHONUNBUFFERED=1

RUN pip install --upgrade pip

WORKDIR /weatherapp-etl/

# Install the project plus the Dagster runtime packages needed in-cluster
RUN pip install . dagster-postgres dagster-k8s dagster-celery dagster-celery-k8s

# Expose the port that your Dagster instance will run on
EXPOSE 80
