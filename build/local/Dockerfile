FROM public.ecr.aws/docker/library/python:3.11 AS builder

RUN apt-get update && \
    apt-get install -y zip && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /package

COPY ./jobs/ /package/jobs/
COPY ./lib/bin/*.jar /package/lib/
COPY ./framework /package/modules/framework
COPY ./framework/requirements.txt /package/

RUN pip install -r requirements.txt -t ./modules
RUN cd modules && zip -r /package/py_deps.zip .

RUN mkdir -p /output/lib && \
    cp /package/jobs/*.py /output/ && \
    cp /package/py_deps.zip /output/ && \
    cp /package/lib/*.jar /output/lib/

FROM scratch AS output
COPY --from=builder /output/ /
