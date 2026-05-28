# ---------- stage 1: maven build ----------
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B -e dependency:go-offline || true
COPY src ./src
RUN mvn -B -DskipTests package

# ---------- stage 2: runtime ----------
FROM apache/spark:3.5.5-java17
USER root
ENV PATH="/opt/spark/bin:/opt/spark/sbin:${PATH}"

# Self-contained ClickHouse JDBC driver (with HttpClient5 transitives bundled).
# Kept outside our maven-shade fat-jar to avoid transitive dep drops.
ADD --chmod=644 https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-all.jar \
    /opt/spark/jars/clickhouse-jdbc.jar

COPY --from=build /src/target/spark-jobs-*.jar /opt/spark/jars/spark-jobs.jar
COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
USER spark
