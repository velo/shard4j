# The coordinator container: the Spring Boot repackaged app jar on the same JRE the
# integration tests run it under. Build the jar first (`mvn -DskipTests package`), then
# build with this module directory as the context.
FROM eclipse-temurin:25-jre

RUN groupadd --system --gid 10001 shard4j \
 && useradd --system --uid 10001 --gid shard4j --home-dir /opt/shard4j \
      --shell /usr/sbin/nologin shard4j \
 && mkdir -p /opt/shard4j /data \
 && chown shard4j:shard4j /opt/shard4j /data

COPY --chown=shard4j:shard4j target/shard4j-coordinator-*-app.jar /opt/shard4j/app.jar

USER shard4j

# The coordinator takes an exclusive lock on the data directory and appends JSONL there;
# it must live on a mount, never in the container layer.
ENV COORDINATOR_DATA_DIR=/data
VOLUME /data

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/opt/shard4j/app.jar"]
