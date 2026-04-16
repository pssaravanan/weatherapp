FROM python:3.11

# Build with the repository root as the context, for example:
# docker build -f infra/eks/Dockerfile .
COPY ./etl /weatherapp-etl

# This makes sure that logs show up immediately instead of being buffered
ENV PYTHONUNBUFFERED=1

RUN pip install --upgrade pip

WORKDIR /weatherapp-etl/

# Install the project so its src-layout package and dependencies are available
RUN pip install .

# Expose the port that your Dagster instance will run on
EXPOSE 80
