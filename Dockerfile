FROM python:3.8-slim

RUN apt-get update && apt-get install -y git graphviz gcc

# until we start rotating tokens, we can leave these steps cached
RUN pip install awscli && \
    buildtoken=$(aws s3 cp s3://pgr-ets-jenkinsx-prod-aws98-secret/eds-build-token -) \
    || buildtoken=$(aws s3 cp s3://pgr-ets-jenkinsp-prod-aws98-secret/eds-build-token -) && \
    pip config set global.index-url \
    https://eds-build:$buildtoken@progressive.jfrog.io/progressive/api/pypi/pgr-pypi/simple

RUN pip install prefect[viz] dask checksumdir pyyaml awscli pgr_prefect

COPY . /app/
WORKDIR /app/

CMD ["/bin/bash"]
