FROM registry.access.redhat.com/ubi9/openjdk-21-runtime:1.20

USER root

RUN mkdir -p /app && \
    chown -R 185:0 /app && \
    chmod -R g=u /app

WORKDIR /app

COPY target/kafka-bridge-*.jar app.jar

USER 185

EXPOSE 8080

ENV JAVA_OPTS="-Xms512m -Xmx1024m"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]