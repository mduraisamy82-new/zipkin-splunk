#
# Copyright The OpenZipkin Authors
# SPDX-License-Identifier: Apache-2.0
#

FROM openjdk:21

ARG SPLUNK_STORAGE_VERSION=2.2.5-SNAPSHOT

ENV ZIPKIN_REPO https://repo1.maven.org/maven2
ENV ZIPKIN_VERSION 3.4.0
ENV ZIPKIN_LOGGING_LEVEL INFO

# Use to set heap, trust store or other system properties.
ENV JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -Dlogging.level.zipkin2=DEBUG
# Add environment settings for supported storage types
ENV STORAGE_TYPE=splunk

WORKDIR /zipkin

RUN curl -SL --insecure $ZIPKIN_REPO/io/zipkin/zipkin-server/$ZIPKIN_VERSION/zipkin-server-${ZIPKIN_VERSION}-exec.jar > zipkin.jar

ADD module/target/zipkin-module-splunk-${SPLUNK_STORAGE_VERSION}-module.jar splunk.jar

EXPOSE 9410 9411

CMD exec java \
    ${JAVA_OPTS} \
    -Dloader.path='splunk.jar,splunk.jar!/lib' \
    -Dspring.profiles.active=${STORAGE_TYPE} \
    -Dlogging.level.org.springframework=DEBUG \
    -cp zipkin.jar \
    org.springframework.boot.loader.launch.PropertiesLauncher