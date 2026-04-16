FROM python:3.11

# Build with the repository root as the context, for example:
# docker build -f infra/eks/Dockerfile .
COPY ./etl /analysis

# This makes sure that logs show up immediately instead of being buffered
ENV PYTHONUNBUFFERED=1

RUN pip install --upgrade pip

# Install dagster and any other dependencies your project requires
RUN \
    pip install \
        dagster \
        dagster-postgres \
        dagster-k8s \
        # add any other dependencies here
        pandas


WORKDIR /analysis/

# Expose the port that your Dagster instance will run on
EXPOSE 80
