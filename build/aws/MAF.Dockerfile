
FROM public.ecr.aws/docker/library/python:3.11 as aws_maf_builder
ARG APPLICATION_FILE
ARG DIST_ARTIFACT_NAME

RUN set -ex; \
  apt-get update && \
  apt-get install -y zip openjdk-21-jdk-headless && \
  rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64

WORKDIR /package

COPY ../jobs/${APPLICATION_FILE} /package/
COPY ../lib/bin/*.jar /package/lib/
COPY ../framework /package/modules/framework
COPY ../requirements.txt /package/


RUN pip install -r requirements.txt -t ./modules
RUN zip -r "${DIST_ARTIFACT_NAME}.zip" ${APPLICATION_FILE} ./lib ./modules ./framework

FROM scratch as output
ARG DIST_ARTIFACT_NAME

COPY --from=aws_maf_builder /package/${DIST_ARTIFACT_NAME}.zip /

