FROM eclipse-temurin:25-jre-noble@sha256:41a70a160a5a157126929c1901e8dcf17ea4fd88eca1e7b9abf3cb1bb04df6c2
ARG P22_SOURCE_REVISION=unrecorded
ARG P22_JAR_SHA256=unrecorded
LABEL portfolio.project="p22" portfolio.source="${P22_SOURCE_REVISION}" portfolio.jar-sha256="${P22_JAR_SHA256}"
ENV P22_SOURCE_REVISION=${P22_SOURCE_REVISION} P22_JAR_SHA256=${P22_JAR_SHA256}
WORKDIR /app
COPY --chown=10001:10001 target/retail-event-stream-1.0.0.jar /app/app.jar
COPY --chown=10001:10001 fixtures /app/fixtures
USER 10001:10001
ENTRYPOINT ["java","-Xms32m","-Xmx192m","-jar","/app/app.jar"]
