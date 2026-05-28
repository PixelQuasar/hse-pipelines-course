# Spark Lakehouse Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a containerized lakehouse pipeline that ingests session logs into Iceberg (bronze), aggregates two business metrics into gold tables, and demonstrates live ingestion via a Rust generator with periodic Spark batch jobs.

**Architecture:** Single docker-compose stack — MinIO (S3) + Nessie (Iceberg REST catalog) + a long-running idle `spark-job` container with the Scala fat-jar + Rust `generator` + `ofelia` scheduler. Spark batch jobs (`ParseJob`, `AggregateJob`) execute via `docker exec` on cron schedule. bronze = typed events, gold = per-day aggregated metrics. Hot-window-recompute (cutoff = 3 days) for incremental aggregation.

**Tech Stack:** Scala 2.12 + Spark 3.5.5 + Iceberg 1.7.0 + Nessie 0.99.0 + Maven 3.9 (Java 17), Rust + aws-sdk-s3 + tokio + encoding_rs, Docker Compose, MinIO, ofelia (`mcuadros/ofelia`).

**Spec reference:** `docs/superpowers/specs/2026-05-23-spark-lakehouse-design.md`

---

## File Structure (locked in before tasks)

```
final/
├── docker-compose.yml                     # Compose 3.8, all services
├── .env                                   # MinIO creds, generator interval
├── data/                                  # existing — 10k input files
├── scheduler/
│   └── config.ini                         # ofelia jobs (parse + aggregate)
├── generator/
│   ├── Cargo.toml
│   ├── Dockerfile                         # multi-stage rust → debian:slim
│   └── src/
│       └── main.rs                        # snapshot dists + loop generator
└── spark-jobs/
    ├── pom.xml                            # Scala/Spark/Iceberg/Nessie deps
    ├── Dockerfile                         # multi-stage maven → bitnami/spark
    ├── conf/
    │   └── spark-defaults.conf            # catalog + S3 + Nessie wiring
    └── src/main/
        ├── scala/ru/consultant/lakehouse/
        │   ├── model/
        │   │   ├── EventType.scala        # event-type constants
        │   │   ├── CardParam.scala        # ($paramId, value)
        │   │   └── RawEvent.scala         # case class + smart constructors
        │   ├── parser/
        │   │   ├── TimeParser.scala       # two ts formats → Option[Instant]
        │   │   ├── EventLineParser.scala  # one line → ParsedLine ADT
        │   │   └── SessionParser.scala    # state machine on session
        │   ├── io/
        │   │   ├── IcebergSchemas.scala   # DDL for bronze.events, processed_files, gold.*
        │   │   ├── BronzeWriter.scala     # append + dedup + processed_files update
        │   │   └── GoldWriter.scala       # MERGE INTO for both metrics
        │   ├── config/
        │   │   └── AppConfig.scala        # paths, table names, cutoff
        │   └── jobs/
        │       ├── SparkApp.scala         # shared SparkSession factory
        │       ├── ParseJob.scala         # raw → bronze
        │       └── AggregateJob.scala     # bronze → gold
        └── resources/
            ├── log4j2.properties
            └── reference.conf             # HOCON defaults
```

---

## Phase 0 — Repository & Git

### Task 0.1: Initialize git repository

**Files:**
- Create: `.gitignore` (in `pipelines-course/` root, if not already)
- Create: `final/.gitignore`

- [ ] **Step 1:** Check git status in repo root

Run: `cd /Users/quasarity/Documents/study/pipelines-course && git status`

If "not a git repository" — proceed to step 2. If repo already exists — skip to step 3.

- [ ] **Step 2:** Initialize repo

Run:
```bash
cd /Users/quasarity/Documents/study/pipelines-course
git init
```

- [ ] **Step 3:** Create `final/.gitignore`

```
# Maven
spark-jobs/target/

# Rust
generator/target/
generator/Cargo.lock

# IDE
.idea/
*.iml
.vscode/

# OS
.DS_Store

# Local data
warehouse/
checkpoints/
**/*.local.env
```

- [ ] **Step 4:** Commit

```bash
cd /Users/quasarity/Documents/study/pipelines-course/final
git add .gitignore docs/
git commit -m "chore: scaffold final/ with design doc and plan"
```

---

## Phase 1 — Minimal infra (MinIO + mc-init + Nessie)

Goal of phase: `docker compose up -d` brings up MinIO and Nessie, mc-init creates buckets and loads the 10k input files into `s3://sessions/`.

### Task 1.1: `final/.env`

**Files:**
- Create: `final/.env`

- [ ] **Step 1:** Write `.env`

```env
# MinIO credentials (used by all services)
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=admin12345

# Generator
GENERATOR_INTERVAL_SECONDS=5

# AWS env (consumed by Spark + generator + mc)
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=admin12345
AWS_REGION=us-east-1
```

- [ ] **Step 2:** Commit

```bash
git add final/.env
git commit -m "chore(infra): add .env with MinIO/AWS credentials"
```

### Task 1.2: docker-compose skeleton (minio + mc-init + nessie)

**Files:**
- Create: `final/docker-compose.yml`

- [ ] **Step 1:** Write compose

```yaml
name: cs-lakehouse

networks:
  lakehouse-net:

volumes:
  minio-data:
  nessie-data:

services:

  minio:
    image: minio/minio:latest
    container_name: minio
    networks: [lakehouse-net]
    ports:
      - "9000:9000"
      - "9001:9001"
    env_file: [.env]
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
    volumes:
      - minio-data:/data
    command: server /data --console-address ":9001"
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 5s
      retries: 10

  mc-init:
    image: minio/mc:latest
    container_name: mc-init
    networks: [lakehouse-net]
    env_file: [.env]
    depends_on:
      minio:
        condition: service_healthy
    volumes:
      - ./data:/seed:ro
    entrypoint: >
      /bin/sh -c "
        mc alias set local http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD} &&
        mc mb -p local/sessions &&
        mc mb -p local/warehouse &&
        echo 'Uploading 10k seed files...' &&
        mc cp --recursive /seed/ local/sessions/ &&
        echo 'mc-init done'
      "
    restart: "no"

  nessie:
    image: ghcr.io/projectnessie/nessie:0.99.0
    container_name: nessie
    networks: [lakehouse-net]
    ports:
      - "19120:19120"
    environment:
      nessie.version.store.type: ROCKSDB
      nessie.version.store.persist.rocks.database-path: /nessie/data
    volumes:
      - nessie-data:/nessie/data
```

- [ ] **Step 2:** Launch and verify MinIO

```bash
cd final
docker compose up -d minio mc-init nessie
```

Wait ~30 sec for mc-init to finish. Then:

```bash
docker compose logs mc-init | tail -5
```

Expected: last line `mc-init done`.

- [ ] **Step 3:** Verify in browser

Open `http://localhost:9001` → login with admin/admin12345 → confirm two buckets `sessions/` and `warehouse/`, and `sessions/` contains ~10000 objects.

Open `http://localhost:19120/tree/main` → Nessie UI loads (empty catalog).

- [ ] **Step 4:** Commit

```bash
git add final/docker-compose.yml
git commit -m "feat(infra): minio + mc-init + nessie services"
```

---

## Phase 2 — Spark application scaffold (Maven + Dockerfile)

Goal of phase: `docker compose build spark-job` produces an image with a packaged Scala fat-jar containing a no-op main class. `docker exec` into the container runs `spark-shell` that connects to Nessie + MinIO.

### Task 2.1: `spark-jobs/pom.xml`

**Files:**
- Create: `final/spark-jobs/pom.xml`

- [ ] **Step 1:** Write `pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>ru.consultant</groupId>
  <artifactId>spark-jobs</artifactId>
  <version>1.0.0</version>
  <packaging>jar</packaging>

  <properties>
    <scala.binary.version>2.12</scala.binary.version>
    <scala.version>2.12.20</scala.version>
    <spark.version>3.5.5</spark.version>
    <iceberg.version>1.7.0</iceberg.version>
    <nessie.version>0.99.0</nessie.version>
    <maven.compiler.source>17</maven.compiler.source>
    <maven.compiler.target>17</maven.compiler.target>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
  </properties>

  <dependencies>
    <!-- Scala -->
    <dependency>
      <groupId>org.scala-lang</groupId>
      <artifactId>scala-library</artifactId>
      <version>${scala.version}</version>
      <scope>provided</scope>
    </dependency>

    <!-- Spark (provided by bitnami/spark image) -->
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-core_${scala.binary.version}</artifactId>
      <version>${spark.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-sql_${scala.binary.version}</artifactId>
      <version>${spark.version}</version>
      <scope>provided</scope>
    </dependency>

    <!-- Iceberg + Nessie + S3 (packaged into fat-jar) -->
    <dependency>
      <groupId>org.apache.iceberg</groupId>
      <artifactId>iceberg-spark-runtime-3.5_${scala.binary.version}</artifactId>
      <version>${iceberg.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.iceberg</groupId>
      <artifactId>iceberg-aws-bundle</artifactId>
      <version>${iceberg.version}</version>
    </dependency>
    <dependency>
      <groupId>org.projectnessie.nessie-integrations</groupId>
      <artifactId>nessie-spark-extensions-3.5_${scala.binary.version}</artifactId>
      <version>${nessie.version}</version>
    </dependency>

    <!-- HOCON config -->
    <dependency>
      <groupId>com.typesafe</groupId>
      <artifactId>config</artifactId>
      <version>1.4.3</version>
    </dependency>
  </dependencies>

  <build>
    <sourceDirectory>src/main/scala</sourceDirectory>
    <plugins>
      <plugin>
        <groupId>net.alchim31.maven</groupId>
        <artifactId>scala-maven-plugin</artifactId>
        <version>4.9.2</version>
        <executions>
          <execution>
            <goals>
              <goal>compile</goal>
              <goal>testCompile</goal>
            </goals>
          </execution>
        </executions>
        <configuration>
          <scalaVersion>${scala.version}</scalaVersion>
          <args>
            <arg>-deprecation</arg>
            <arg>-feature</arg>
          </args>
        </configuration>
      </plugin>

      <plugin>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.3</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <createDependencyReducedPom>false</createDependencyReducedPom>
              <shadedArtifactAttached>false</shadedArtifactAttached>
              <filters>
                <filter>
                  <artifact>*:*</artifact>
                  <excludes>
                    <exclude>META-INF/*.SF</exclude>
                    <exclude>META-INF/*.DSA</exclude>
                    <exclude>META-INF/*.RSA</exclude>
                  </excludes>
                </filter>
              </filters>
              <transformers>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
              </transformers>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

### Task 2.2: Resources

**Files:**
- Create: `final/spark-jobs/src/main/resources/log4j2.properties`
- Create: `final/spark-jobs/src/main/resources/reference.conf`

- [ ] **Step 1:** Write `log4j2.properties`

```properties
status = warn
appenders = console
appender.console.type = Console
appender.console.name = STDOUT
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{HH:mm:ss} %-5p %c{1.} - %m%n

rootLogger.level = warn
rootLogger.appenderRefs = stdout
rootLogger.appenderRef.stdout.ref = STDOUT

logger.lakehouse.name = ru.consultant.lakehouse
logger.lakehouse.level = info
```

- [ ] **Step 2:** Write `reference.conf`

```hocon
app {
  warehouse-catalog = "warehouse"

  bronze {
    events-table = "warehouse.bronze.events"
    processed-files-table = "warehouse.bronze.processed_files"
  }

  gold {
    card-hits-table = "warehouse.gold.card_search_doc_hits_daily"
    qs-opens-table = "warehouse.gold.qs_doc_opens_daily"
  }

  sources {
    sessions-prefix = "s3a://sessions/"
  }

  aggregate {
    cutoff-days = 3
  }
}
```

### Task 2.3: Stub main class so Maven builds

**Files:**
- Create: `final/spark-jobs/src/main/scala/ru/consultant/lakehouse/jobs/ParseJob.scala`
- Create: `final/spark-jobs/src/main/scala/ru/consultant/lakehouse/jobs/AggregateJob.scala`

- [ ] **Step 1:** Stub `ParseJob.scala`

```scala
package ru.consultant.lakehouse.jobs

object ParseJob {
  def main(args: Array[String]): Unit = {
    println("ParseJob stub — to be implemented")
  }
}
```

- [ ] **Step 2:** Stub `AggregateJob.scala`

```scala
package ru.consultant.lakehouse.jobs

object AggregateJob {
  def main(args: Array[String]): Unit = {
    println("AggregateJob stub — to be implemented")
  }
}
```

### Task 2.4: `spark-defaults.conf`

**Files:**
- Create: `final/spark-jobs/conf/spark-defaults.conf`

- [ ] **Step 1:** Write defaults

```properties
# Iceberg extension + Nessie
spark.sql.extensions                                              org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions

# Iceberg catalog named "warehouse", backed by Nessie + S3FileIO
spark.sql.catalog.warehouse                                       org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.warehouse.catalog-impl                          org.apache.iceberg.nessie.NessieCatalog
spark.sql.catalog.warehouse.uri                                   http://nessie:19120/api/v1
spark.sql.catalog.warehouse.ref                                   main
spark.sql.catalog.warehouse.warehouse                             s3://warehouse/
spark.sql.catalog.warehouse.io-impl                               org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.warehouse.s3.endpoint                           http://minio:9000
spark.sql.catalog.warehouse.s3.path-style-access                  true

# Hadoop S3A confs (for direct s3a:// reads of raw sessions)
spark.hadoop.fs.s3a.endpoint                                      http://minio:9000
spark.hadoop.fs.s3a.path.style.access                             true
spark.hadoop.fs.s3a.connection.ssl.enabled                        false
spark.hadoop.fs.s3a.aws.credentials.provider                      org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider

# Default catalog
spark.sql.defaultCatalog                                          warehouse
```

### Task 2.5: `spark-jobs/Dockerfile`

**Files:**
- Create: `final/spark-jobs/Dockerfile`

- [ ] **Step 1:** Write multi-stage Dockerfile

```dockerfile
# ---------- stage 1: maven build ----------
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
# warm dep cache (best-effort; ok if it fails on tricky deps)
RUN mvn -B -e dependency:go-offline || true
COPY src ./src
RUN mvn -B -DskipTests package

# ---------- stage 2: runtime ----------
FROM bitnami/spark:3.5
USER root
RUN mkdir -p /app/jars
COPY --from=build /src/target/spark-jobs-*.jar /app/jars/spark-jobs.jar
COPY conf/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
# AWS creds are passed via env at runtime (see compose)
USER 1001
```

### Task 2.6: Wire `spark-job` service into compose

**Files:**
- Modify: `final/docker-compose.yml`

- [ ] **Step 1:** Append `spark-job` service before final closing of `services:` section

```yaml
  spark-job:
    build:
      context: ./spark-jobs
      dockerfile: Dockerfile
    image: cs-lakehouse/spark-job:latest
    container_name: spark-job
    networks: [lakehouse-net]
    depends_on:
      mc-init: { condition: service_completed_successfully }
      nessie:  { condition: service_started }
    env_file: [.env]
    environment:
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
      AWS_REGION: ${AWS_REGION}
    # Long-running idle JVM; scheduler does `docker exec spark-job spark-submit ...`
    entrypoint: ["tail", "-f", "/dev/null"]
    restart: unless-stopped
```

- [ ] **Step 2:** Build the image

```bash
cd final
docker compose build spark-job
```

Expected: build succeeds in ~5-15 min on first run (downloads Maven deps).

- [ ] **Step 3:** Smoke test — run stub job

```bash
docker compose up -d spark-job
docker compose exec spark-job spark-submit \
  --class ru.consultant.lakehouse.jobs.ParseJob \
  /app/jars/spark-jobs.jar
```

Expected output contains: `ParseJob stub — to be implemented`.

- [ ] **Step 4:** Smoke test — Spark + Iceberg + Nessie connectivity via spark-sql

```bash
docker compose exec spark-job spark-sql -e "SHOW NAMESPACES IN warehouse;"
```

Expected: command succeeds (empty output or "default" namespace). If error mentions `connection refused` to `nessie:19120` — verify nessie container is up and ports right.

- [ ] **Step 5:** Commit

```bash
git add final/spark-jobs final/docker-compose.yml
git commit -m "feat(spark): scaffold Maven project + spark-job service"
```

---

## Phase 3 — Parser layer (pure Scala, no Spark)

Goal of phase: a `SessionParser.parse(sessionId, content): Seq[RawEvent]` function that correctly handles real session files from `final/data/`. No Spark imports anywhere in `parser/` or `model/`.

### Task 3.1: `model/EventType.scala`

**Files:**
- Create: `final/spark-jobs/src/main/scala/ru/consultant/lakehouse/model/EventType.scala`

- [ ] **Step 1:** Write file

```scala
package ru.consultant.lakehouse.model

object EventType {
  val SessionStart = "SESSION_START"
  val SessionEnd   = "SESSION_END"
  val Qs           = "QS"
  val CardSearch   = "CARD_SEARCH"
  val DocOpen      = "DOC_OPEN"
  val Malformed    = "MALFORMED"
}

object SearchKind {
  val Qs   = "QS"
  val Card = "CARD"
}
```

### Task 3.2: `model/CardParam.scala`

**Files:**
- Create: `final/spark-jobs/src/main/scala/ru/consultant/lakehouse/model/CardParam.scala`

- [ ] **Step 1:** Write file

```scala
package ru.consultant.lakehouse.model

final case class CardParam(paramId: String, value: String)
```

### Task 3.3: `model/RawEvent.scala`

**Files:**
- Create: `final/spark-jobs/src/main/scala/ru/consultant/lakehouse/model/RawEvent.scala`

- [ ] **Step 1:** Write file

```scala
package ru.consultant.lakehouse.model

import java.time.Instant

final case class RawEvent(
  sessionId:    String,
  eventSeq:     Int,
  eventTime:    Option[Instant],
  eventType:    String,
  searchId:     Option[String],
  searchKind:   Option[String],
  queryText:    Option[String],
  cardParams:   Seq[CardParam],
  resultDocIds: Seq[String],
  docId:        Option[String],
  parseError:   Option[String],
  rawLine:      Option[String]
)

object RawEvent {
  private def empty(sid: String, seq: Int): RawEvent =
    RawEvent(sid, seq, None, "", None, None, None, Nil, Nil, None, None, None)

  def sessionStart(sid: String, seq: Int, ts: Instant): RawEvent =
    empty(sid, seq).copy(eventTime = Some(ts), eventType = EventType.SessionStart)

  def sessionEnd(sid: String, seq: Int, ts: Instant): RawEvent =
    empty(sid, seq).copy(eventTime = Some(ts), eventType = EventType.SessionEnd)

  def qs(sid: String, seq: Int, ts: Instant, searchId: String, query: String, docs: Seq[String]): RawEvent =
    empty(sid, seq).copy(
      eventTime    = Some(ts),
      eventType    = EventType.Qs,
      searchId     = Some(searchId),
      searchKind   = Some(SearchKind.Qs),
      queryText    = Some(query),
      resultDocIds = docs
    )

  def cardSearch(sid: String, seq: Int, ts: Instant, searchId: String,
                 params: Seq[CardParam], docs: Seq[String]): RawEvent =
    empty(sid, seq).copy(
      eventTime    = Some(ts),
      eventType    = EventType.CardSearch,
      searchId     = Some(searchId),
      searchKind   = Some(SearchKind.Card),
      cardParams   = params,
      resultDocIds = docs
    )

  def docOpen(sid: String, seq: Int, ts: Instant, searchId: String, docId: String,
              resolvedKind: Option[String]): RawEvent =
    empty(sid, seq).copy(
      eventTime  = Some(ts),
      eventType  = EventType.DocOpen,
      searchId   = Some(searchId),
      searchKind = resolvedKind,
      docId      = Some(docId)
    )

  def malformed(sid: String, seq: Int, raw: String, err: String): RawEvent =
    empty(sid, seq).copy(
      eventType  = EventType.Malformed,
      parseError = Some(err),
      rawLine    = Some(raw)
    )
}
```

### Task 3.4: `parser/TimeParser.scala`

**Files:**
- Create: `final/spark-jobs/src/main/scala/ru/consultant/lakehouse/parser/TimeParser.scala`

- [ ] **Step 1:** Write file

```scala
package ru.consultant.lakehouse.parser

import java.time.{Instant, LocalDateTime, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Locale

object TimeParser {
  private val Primary = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
  private val Rfc     = DateTimeFormatter.ofPattern("EEE,_dd_MMM_yyyy_HH:mm:ss_Z", Locale.ENGLISH)

  def parse(s: String): Option[Instant] =
    tryLocal(s, Primary).orElse(tryZoned(s, Rfc))

  private def tryLocal(s: String, fmt: DateTimeFormatter): Option[Instant] =
    try Some(LocalDateTime.parse(s, fmt).toInstant(ZoneOffset.UTC))
    catch { case _: Throwable => None }

  private def tryZoned(s: String, fmt: DateTimeFormatter): Option[Instant] =
    try Some(ZonedDateTime.parse(s, fmt).toInstant)
    catch { case _: Throwable => None }
}
```

### Task 3.5: `parser/EventLineParser.scala`

**Files:**
- Create: `final/spark-jobs/src/main/scala/ru/consultant/lakehouse/parser/EventLineParser.scala`

- [ ] **Step 1:** Write file

```scala
package ru.consultant.lakehouse.parser

import java.time.Instant
import ru.consultant.lakehouse.model.CardParam

sealed trait ParsedLine
object ParsedLine {
  final case class  SessionStart(ts: Instant)                              extends ParsedLine
  final case class  SessionEnd(ts: Instant)                                extends ParsedLine
  final case class  QsHeader(ts: Instant, query: String)                   extends ParsedLine
  final case class  CardSearchStart(ts: Instant)                           extends ParsedLine
  case object       CardSearchEnd                                          extends ParsedLine
  final case class  DocOpen(ts: Instant, searchId: String, docId: String)  extends ParsedLine
  final case class  CardParamLine(p: CardParam)                            extends ParsedLine
  final case class  SearchResults(searchId: String, docIds: Seq[String])   extends ParsedLine
  final case class  Malformed(raw: String, error: String)                  extends ParsedLine
}

object EventLineParser {
  import ParsedLine._

  private val SessionStartRx = """^SESSION_START\s+(\S+)\s*$""".r
  private val SessionEndRx   = """^SESSION_END\s+(\S+)\s*$""".r
  private val QsRx           = """^QS\s+(\S+)\s+\{(.*)\}\s*$""".r
  private val CardStartRx    = """^CARD_SEARCH_START\s+(\S+)\s*$""".r
  private val DocOpenRx      = """^DOC_OPEN\s+(\S+)\s+(\S+)\s+(\S+)\s*$""".r
  private val CardParamRx    = """^\$(\S+)\s+(.+)$""".r
  private val ResultsRx      = """^(\d+)((?:\s+\S+)*)\s*$""".r

  def parse(line: String): ParsedLine = line match {
    case SessionStartRx(ts)      => withTs(ts, line, SessionStart.apply)
    case SessionEndRx(ts)        => withTs(ts, line, SessionEnd.apply)
    case QsRx(ts, query)         => withTs(ts, line, t => QsHeader(t, query))
    case CardStartRx(ts)         => withTs(ts, line, CardSearchStart.apply)
    case "CARD_SEARCH_END"       => CardSearchEnd
    case DocOpenRx(ts, sid, did) => withTs(ts, line, t => DocOpen(t, sid, did))
    case CardParamRx(pid, value) => CardParamLine(CardParam(pid, value.trim))
    case ResultsRx(sid, docs)    => SearchResults(sid, docs.trim.split("""\s+""").filter(_.nonEmpty).toSeq)
    case other                   => Malformed(other, "unrecognized prefix")
  }

  private def withTs(s: String, raw: String, f: Instant => ParsedLine): ParsedLine =
    TimeParser.parse(s) match {
      case Some(t) => f(t)
      case None    => Malformed(raw, s"unparseable timestamp: $s")
    }
}
```

### Task 3.6: `parser/SessionParser.scala`

**Files:**
- Create: `final/spark-jobs/src/main/scala/ru/consultant/lakehouse/parser/SessionParser.scala`

- [ ] **Step 1:** Write file

```scala
package ru.consultant.lakehouse.parser

import ru.consultant.lakehouse.model.{CardParam, RawEvent, SearchKind}
import java.time.Instant

object SessionParser {

  private sealed trait State
  private object State {
    case object Neutral                                                        extends State
    final case class AwaitingQsResults(ts: Instant, query: String)             extends State
    final case class InCardSearch(ts: Instant, params: Vector[CardParam])      extends State
    final case class AwaitingCardResults(ts: Instant, params: Vector[CardParam]) extends State
  }

  /** Parse one session file's content into atomic events.
    * Also resolves DOC_OPEN's searchKind by remembering search_id → kind within this session.
    */
  def parse(sessionId: String, content: String): Seq[RawEvent] = {
    val out      = scala.collection.mutable.ArrayBuffer.empty[RawEvent]
    var state: State = State.Neutral
    var seq      = 0
    // search_id → SearchKind seen earlier in this session
    val kindBySearchId = scala.collection.mutable.HashMap.empty[String, String]

    content.linesIterator.foreach { raw =>
      val line = raw.trim
      if (line.nonEmpty) {
        val (emitted, newState) = step(state, EventLineParser.parse(line), line, sessionId, seq, kindBySearchId)
        emitted.foreach { ev =>
          out += ev
          seq += 1
        }
        state = newState
      }
    }
    out.toSeq
  }

  private def step(
    state:           State,
    parsed:          ParsedLine,
    raw:             String,
    sessionId:       String,
    seq:             Int,
    kindBySearchId:  scala.collection.mutable.HashMap[String, String]
  ): (Seq[RawEvent], State) = {
    import ParsedLine._
    import State._

    (state, parsed) match {

      // Session boundaries reset state
      case (_, SessionStart(ts)) =>
        (Seq(RawEvent.sessionStart(sessionId, seq, ts)), Neutral)
      case (_, SessionEnd(ts)) =>
        (Seq(RawEvent.sessionEnd(sessionId, seq, ts)), Neutral)

      // QS
      case (_, QsHeader(ts, query)) =>
        (Nil, AwaitingQsResults(ts, query))
      case (AwaitingQsResults(ts, q), SearchResults(sid, docs)) =>
        kindBySearchId.update(sid, SearchKind.Qs)
        (Seq(RawEvent.qs(sessionId, seq, ts, sid, q, docs)), Neutral)

      // CARD search
      case (_, CardSearchStart(ts)) =>
        (Nil, InCardSearch(ts, Vector.empty))
      case (InCardSearch(ts, ps), CardParamLine(p)) =>
        (Nil, InCardSearch(ts, ps :+ p))
      case (InCardSearch(ts, ps), CardSearchEnd) =>
        (Nil, AwaitingCardResults(ts, ps))
      case (AwaitingCardResults(ts, ps), SearchResults(sid, docs)) =>
        kindBySearchId.update(sid, SearchKind.Card)
        (Seq(RawEvent.cardSearch(sessionId, seq, ts, sid, ps, docs)), Neutral)

      // DOC_OPEN — always emit; resolve kind via kindBySearchId
      case (st, DocOpen(ts, sid, did)) =>
        val kind = kindBySearchId.get(sid)
        (Seq(RawEvent.docOpen(sessionId, seq, ts, sid, did, kind)), st)

      // Malformed line from EventLineParser
      case (st, Malformed(_, err)) =>
        (Seq(RawEvent.malformed(sessionId, seq, raw, err)), st)

      // Anything else is unexpected for the current state
      case (st, unexpected) =>
        val err = s"unexpected '${unexpected.getClass.getSimpleName}' in state ${st.getClass.getSimpleName}"
        (Seq(RawEvent.malformed(sessionId, seq, raw, err)), st)
    }
  }
}
```

### Task 3.7: Smoke-test parser via mvn compile + ad-hoc main

**Files:**
- Create: `final/spark-jobs/src/main/scala/ru/consultant/lakehouse/parser/ParserDemo.scala`

- [ ] **Step 1:** Write demo

```scala
package ru.consultant.lakehouse.parser

import scala.io.Source
import java.nio.charset.Charset

object ParserDemo {
  def main(args: Array[String]): Unit = {
    val path = args.headOption.getOrElse("/seed/0")
    val bytes = java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(path))
    val content = new String(bytes, Charset.forName("Cp1251"))
    val sessionId = path.split("/").last
    val events = SessionParser.parse(sessionId, content)
    events.foreach(println)
    println(s"---- total events: ${events.size}")
  }
}
```

- [ ] **Step 2:** Rebuild image

```bash
cd final
docker compose build spark-job
docker compose up -d spark-job
```

- [ ] **Step 3:** Run demo against `data/0`

We need to make `final/data/0` visible inside the container. Easiest: temporarily exec into container and run with bundled jar; mount data via additional volume.

Modify `spark-job` service in `docker-compose.yml` to add:

```yaml
    volumes:
      - ./data:/seed:ro
```

Then:

```bash
docker compose up -d spark-job
docker compose exec spark-job spark-submit \
  --class ru.consultant.lakehouse.parser.ParserDemo \
  /app/jars/spark-jobs.jar /seed/0
```

Expected: output shows ~13 events including `SESSION_START`, one `QS` event, several `DOC_OPEN` events, `SESSION_END`. Last line: `---- total events: <N>`.

- [ ] **Step 4:** Run against `data/95` (card + qs)

```bash
docker compose exec spark-job spark-submit \
  --class ru.consultant.lakehouse.parser.ParserDemo \
  /app/jars/spark-jobs.jar /seed/95
```

Expected: contains one `CARD_SEARCH` event (with `cardParams = List(CardParam(0,RLAW020_82033))`, `resultDocIds = List(RLAW020_82033)`), one `QS`, several `DOC_OPEN` (the first one's `searchKind=Some(CARD)`, later ones `Some(QS)`).

- [ ] **Step 5:** Commit

```bash
git add final/spark-jobs/src/main/scala final/docker-compose.yml
git commit -m "feat(parser): TimeParser, EventLineParser, SessionParser + demo"
```

---

## Phase 4 — Iceberg schemas + ParseJob

Goal: `docker compose exec spark-job spark-submit ParseJob` reads all sessions from S3, parses them, writes to `bronze.events` and `bronze.processed_files`. Idempotent: second run is a no-op.

### Task 4.1: `io/IcebergSchemas.scala`

**Files:**
- Create: `final/spark-jobs/src/main/scala/ru/consultant/lakehouse/io/IcebergSchemas.scala`

- [ ] **Step 1:** Write file

```scala
package ru.consultant.lakehouse.io

import org.apache.spark.sql.SparkSession

object IcebergSchemas {

  def ensureNamespaces(spark: SparkSession): Unit = {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS warehouse.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS warehouse.gold")
  }

  def ensureBronzeEvents(spark: SparkSession): Unit = {
    spark.sql("""
      CREATE TABLE IF NOT EXISTS warehouse.bronze.events (
        session_id      STRING,
        event_seq       INT,
        event_time      TIMESTAMP,
        event_date      DATE,
        event_type      STRING,
        search_id       STRING,
        search_kind     STRING,
        query_text      STRING,
        card_params     ARRAY<STRUCT<param_id: STRING, value: STRING>>,
        result_doc_ids  ARRAY<STRING>,
        doc_id          STRING,
        parse_error     STRING,
        raw_line        STRING
      )
      USING iceberg
      PARTITIONED BY (event_date)
      TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'zstd'
      )
    """)
  }

  def ensureProcessedFiles(spark: SparkSession): Unit = {
    spark.sql("""
      CREATE TABLE IF NOT EXISTS warehouse.bronze.processed_files (
        filename     STRING,
        processed_at TIMESTAMP
      )
      USING iceberg
    """)
  }

  def ensureGoldCardHits(spark: SparkSession): Unit = {
    spark.sql("""
      CREATE TABLE IF NOT EXISTS warehouse.gold.card_search_doc_hits_daily (
        date         DATE,
        doc_id       STRING,
        hits         BIGINT,
        computed_at  TIMESTAMP
      )
      USING iceberg
      PARTITIONED BY (date)
    """)
  }

  def ensureGoldQsOpens(spark: SparkSession): Unit = {
    spark.sql("""
      CREATE TABLE IF NOT EXISTS warehouse.gold.qs_doc_opens_daily (
        open_date    DATE,
        doc_id       STRING,
        opens        BIGINT,
        computed_at  TIMESTAMP
      )
      USING iceberg
      PARTITIONED BY (open_date)
    """)
  }

  def ensureAll(spark: SparkSession): Unit = {
    ensureNamespaces(spark)
    ensureBronzeEvents(spark)
    ensureProcessedFiles(spark)
    ensureGoldCardHits(spark)
    ensureGoldQsOpens(spark)
  }
}
```

### Task 4.2: `config/AppConfig.scala`

**Files:**
- Create: `final/spark-jobs/src/main/scala/ru/consultant/lakehouse/config/AppConfig.scala`

- [ ] **Step 1:** Write file

```scala
package ru.consultant.lakehouse.config

import com.typesafe.config.{Config, ConfigFactory}

final case class AppConfig(
  warehouseCatalog:    String,
  bronzeEventsTable:   String,
  processedFilesTable: String,
  cardHitsTable:       String,
  qsOpensTable:        String,
  sessionsPrefix:      String,
  cutoffDays:          Int
)

object AppConfig {
  def load(): AppConfig = {
    val c: Config = ConfigFactory.load().getConfig("app")
    AppConfig(
      warehouseCatalog    = c.getString("warehouse-catalog"),
      bronzeEventsTable   = c.getString("bronze.events-table"),
      processedFilesTable = c.getString("bronze.processed-files-table"),
      cardHitsTable       = c.getString("gold.card-hits-table"),
      qsOpensTable        = c.getString("gold.qs-opens-table"),
      sessionsPrefix      = c.getString("sources.sessions-prefix"),
      cutoffDays          = c.getInt("aggregate.cutoff-days")
    )
  }
}
```

### Task 4.3: `jobs/SparkApp.scala`

**Files:**
- Create: `final/spark-jobs/src/main/scala/ru/consultant/lakehouse/jobs/SparkApp.scala`

- [ ] **Step 1:** Write file

```scala
package ru.consultant.lakehouse.jobs

import org.apache.spark.sql.SparkSession

object SparkApp {
  def session(appName: String): SparkSession =
    SparkSession.builder().appName(appName).getOrCreate()
}
```

(All catalog/S3/Nessie configs are in `spark-defaults.conf`.)

### Task 4.4: `io/BronzeWriter.scala`

**Files:**
- Create: `final/spark-jobs/src/main/scala/ru/consultant/lakehouse/io/BronzeWriter.scala`

- [ ] **Step 1:** Write file

```scala
package ru.consultant.lakehouse.io

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object BronzeWriter {

  /** Append events DF to bronze.events, deduped on (session_id, event_seq).
    * Note: dedup is done within the incoming batch; existing rows in bronze are
    * NOT re-deduped (would be expensive scan). Re-processing of the same file
    * is prevented at the processed_files layer.
    */
  def appendEvents(spark: SparkSession, events: DataFrame): Unit = {
    val deduped = events.dropDuplicates("session_id", "event_seq")
    deduped.writeTo("warehouse.bronze.events").append()
  }

  /** Mark filenames as processed so subsequent ParseJob runs skip them. */
  def markProcessed(spark: SparkSession, filenames: Seq[String]): Unit = {
    if (filenames.nonEmpty) {
      import spark.implicits._
      val df = filenames.toDF("filename")
        .withColumn("processed_at", current_timestamp())
      df.writeTo("warehouse.bronze.processed_files").append()
    }
  }

  /** Returns filenames already in bronze.processed_files. */
  def alreadyProcessed(spark: SparkSession): Set[String] = {
    import spark.implicits._
    spark.table("warehouse.bronze.processed_files")
      .select("filename")
      .as[String]
      .collect()
      .toSet
  }
}
```

### Task 4.5: `jobs/ParseJob.scala`

**Files:**
- Modify: `final/spark-jobs/src/main/scala/ru/consultant/lakehouse/jobs/ParseJob.scala`

- [ ] **Step 1:** Replace stub with full implementation

```scala
package ru.consultant.lakehouse.jobs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ru.consultant.lakehouse.config.AppConfig
import ru.consultant.lakehouse.io.{BronzeWriter, IcebergSchemas}
import ru.consultant.lakehouse.model.{CardParam, RawEvent}
import ru.consultant.lakehouse.parser.SessionParser

object ParseJob {

  def main(args: Array[String]): Unit = {
    val cfg = AppConfig.load()
    val spark = SparkApp.session("ParseJob")
    import spark.implicits._

    IcebergSchemas.ensureAll(spark)

    // 1. List all session files under s3a://sessions/
    val sourcePath = cfg.sessionsPrefix.stripSuffix("/")
    val fs = FileSystem.get(java.net.URI.create(sourcePath), spark.sparkContext.hadoopConfiguration)
    val allPaths: Seq[String] = {
      val it = fs.listFiles(new Path(sourcePath), /*recursive=*/true)
      val buf = scala.collection.mutable.ArrayBuffer.empty[String]
      while (it.hasNext) {
        val st = it.next()
        if (st.isFile) buf += st.getPath.toString
      }
      buf.toSeq
    }

    // 2. Anti-join with processed_files
    val alreadyDone = BronzeWriter.alreadyProcessed(spark)
    val newPaths = allPaths.filterNot(alreadyDone.contains)
    println(s"[ParseJob] discovered ${allPaths.size} total, ${newPaths.size} new")

    if (newPaths.isEmpty) {
      println("[ParseJob] nothing new to do")
      spark.stop()
      return
    }

    // 3. Read each new file as bytes, decode cp1251, parse to events
    val pathsRdd = spark.sparkContext.parallelize(newPaths, math.min(newPaths.size, 64))
    val eventsRdd = pathsRdd.flatMap { fullPath =>
      val hadoopConf = new Configuration() // serialization-safe new conf
      hadoopConf.set("fs.s3a.endpoint", System.getProperty("fs.s3a.endpoint", "http://minio:9000"))
      val fs2 = FileSystem.get(java.net.URI.create(fullPath), hadoopConf)
      val in = fs2.open(new Path(fullPath))
      try {
        val bytes = org.apache.hadoop.io.IOUtils.toByteArray(in)
        val content = new String(bytes, java.nio.charset.Charset.forName("Cp1251"))
        val sessionId = fullPath.split("/").last
        SessionParser.parse(sessionId, content)
      } finally in.close()
    }

    // 4. Convert to DataFrame with explicit schema for Iceberg
    val rows = eventsRdd.map(toRow)
    val df = spark.createDataFrame(rows, BronzeRowSchema)
      .withColumn("event_date", to_date($"event_time"))
      // Reorder columns to match table DDL
      .select(
        $"session_id", $"event_seq", $"event_time", $"event_date",
        $"event_type", $"search_id", $"search_kind", $"query_text",
        $"card_params", $"result_doc_ids", $"doc_id", $"parse_error", $"raw_line"
      )

    // 5. Append to bronze + mark processed (two writes; at-least-once semantics)
    BronzeWriter.appendEvents(spark, df)
    BronzeWriter.markProcessed(spark, newPaths)

    println(s"[ParseJob] done; appended events for ${newPaths.size} files")
    spark.stop()
  }

  // -- helpers --

  private val CardParamStruct = StructType(Seq(
    StructField("param_id", StringType, nullable = true),
    StructField("value",    StringType, nullable = true)
  ))

  private val BronzeRowSchema = StructType(Seq(
    StructField("session_id",     StringType,                                                      nullable = true),
    StructField("event_seq",      IntegerType,                                                     nullable = false),
    StructField("event_time",     TimestampType,                                                   nullable = true),
    StructField("event_type",     StringType,                                                      nullable = true),
    StructField("search_id",      StringType,                                                      nullable = true),
    StructField("search_kind",    StringType,                                                      nullable = true),
    StructField("query_text",     StringType,                                                      nullable = true),
    StructField("card_params",    ArrayType(CardParamStruct, containsNull = false),                nullable = true),
    StructField("result_doc_ids", ArrayType(StringType, containsNull = false),                     nullable = true),
    StructField("doc_id",         StringType,                                                      nullable = true),
    StructField("parse_error",    StringType,                                                      nullable = true),
    StructField("raw_line",       StringType,                                                      nullable = true)
  ))

  private def toRow(e: RawEvent): Row = Row(
    e.sessionId,
    e.eventSeq,
    e.eventTime.map(i => java.sql.Timestamp.from(i)).orNull,
    e.eventType,
    e.searchId.orNull,
    e.searchKind.orNull,
    e.queryText.orNull,
    e.cardParams.map(p => Row(p.paramId, p.value)),
    e.resultDocIds,
    e.docId.orNull,
    e.parseError.orNull,
    e.rawLine.orNull
  )
}
```

- [ ] **Step 2:** Rebuild image

```bash
cd final
docker compose build spark-job
docker compose up -d spark-job
```

- [ ] **Step 3:** Run ParseJob

```bash
docker compose exec spark-job spark-submit \
  --class ru.consultant.lakehouse.jobs.ParseJob \
  /app/jars/spark-jobs.jar
```

Expected (last lines):
```
[ParseJob] discovered 10000 total, 10000 new
[ParseJob] done; appended events for 10000 files
```

- [ ] **Step 4:** Verify bronze populated

```bash
docker compose exec spark-job spark-sql -e \
  "SELECT event_type, count(*) FROM warehouse.bronze.events GROUP BY event_type ORDER BY 1"
```

Expected: rows for `SESSION_START`, `SESSION_END`, `QS`, `CARD_SEARCH`, `DOC_OPEN`, maybe `MALFORMED`. Total = sum should be ~few hundred thousand for 10k sessions.

```bash
docker compose exec spark-job spark-sql -e \
  "SELECT count(*) FROM warehouse.bronze.processed_files"
```

Expected: `10000`.

- [ ] **Step 5:** Verify idempotency — re-run should be no-op

```bash
docker compose exec spark-job spark-submit \
  --class ru.consultant.lakehouse.jobs.ParseJob \
  /app/jars/spark-jobs.jar
```

Expected: `discovered 10000 total, 0 new` then `nothing new to do`.

- [ ] **Step 6:** Commit

```bash
git add final/spark-jobs/src/main/scala
git commit -m "feat(spark): ParseJob with bronze.events + processed_files tracking"
```

---

## Phase 5 — AggregateJob

Goal: compute both target metrics into gold tables via MERGE INTO. Cutoff-based hot-window recompute. Bootstrap (gold empty) ⇒ full recompute automatically (since no rows older than cutoff are written/lost).

### Task 5.1: `io/GoldWriter.scala`

**Files:**
- Create: `final/spark-jobs/src/main/scala/ru/consultant/lakehouse/io/GoldWriter.scala`

- [ ] **Step 1:** Write file

```scala
package ru.consultant.lakehouse.io

import org.apache.spark.sql.SparkSession
import java.time.LocalDate

object GoldWriter {

  /** Compute card_search_doc_hits_daily for events with event_date >= cutoff and MERGE into gold.
    * Overwrite semantics: for any (date, doc_id) key in the delta, the new count replaces the old.
    */
  def upsertCardHits(spark: SparkSession, cutoff: LocalDate): Unit = {
    spark.sql(s"""
      WITH delta AS (
        SELECT
          event_date AS date,
          exploded   AS doc_id,
          count(*)   AS hits
        FROM warehouse.bronze.events
        LATERAL VIEW explode(result_doc_ids) AS exploded
        WHERE event_type = 'CARD_SEARCH'
          AND event_date >= DATE('$cutoff')
        GROUP BY event_date, exploded
      )
      MERGE INTO warehouse.gold.card_search_doc_hits_daily AS g
      USING delta AS d
      ON g.date = d.date AND g.doc_id = d.doc_id
      WHEN MATCHED THEN UPDATE SET hits = d.hits, computed_at = current_timestamp()
      WHEN NOT MATCHED THEN INSERT (date, doc_id, hits, computed_at)
                          VALUES (d.date, d.doc_id, d.hits, current_timestamp())
    """)
  }

  /** Compute qs_doc_opens_daily for events with event_date >= cutoff and MERGE into gold. */
  def upsertQsOpens(spark: SparkSession, cutoff: LocalDate): Unit = {
    spark.sql(s"""
      WITH delta AS (
        SELECT
          event_date AS open_date,
          doc_id,
          count(*) AS opens
        FROM warehouse.bronze.events
        WHERE event_type = 'DOC_OPEN'
          AND search_kind = 'QS'
          AND event_date >= DATE('$cutoff')
          AND doc_id IS NOT NULL
        GROUP BY event_date, doc_id
      )
      MERGE INTO warehouse.gold.qs_doc_opens_daily AS g
      USING delta AS d
      ON g.open_date = d.open_date AND g.doc_id = d.doc_id
      WHEN MATCHED THEN UPDATE SET opens = d.opens, computed_at = current_timestamp()
      WHEN NOT MATCHED THEN INSERT (open_date, doc_id, opens, computed_at)
                          VALUES (d.open_date, d.doc_id, d.opens, current_timestamp())
    """)
  }
}
```

### Task 5.2: `jobs/AggregateJob.scala`

**Files:**
- Modify: `final/spark-jobs/src/main/scala/ru/consultant/lakehouse/jobs/AggregateJob.scala`

- [ ] **Step 1:** Replace stub with full implementation

```scala
package ru.consultant.lakehouse.jobs

import ru.consultant.lakehouse.config.AppConfig
import ru.consultant.lakehouse.io.{GoldWriter, IcebergSchemas}
import java.time.LocalDate

object AggregateJob {

  def main(args: Array[String]): Unit = {
    val cfg = AppConfig.load()
    val mode = parseMode(args).getOrElse("incremental")
    val cutoffDays = parseCutoffDays(args).getOrElse(cfg.cutoffDays)

    val spark = SparkApp.session(s"AggregateJob[$mode]")
    IcebergSchemas.ensureAll(spark)

    val cutoff: LocalDate = mode match {
      case "full" => LocalDate.parse("1970-01-01")
      case _      => LocalDate.now().minusDays(cutoffDays.toLong)
    }

    println(s"[AggregateJob] mode=$mode cutoff=$cutoff")

    GoldWriter.upsertCardHits(spark, cutoff)
    GoldWriter.upsertQsOpens(spark, cutoff)

    println("[AggregateJob] done")
    spark.stop()
  }

  private def parseMode(args: Array[String]): Option[String] =
    args.collectFirst { case a if a.startsWith("--mode=") => a.stripPrefix("--mode=") }

  private def parseCutoffDays(args: Array[String]): Option[Int] =
    args.collectFirst { case a if a.startsWith("--cutoff-days=") => a.stripPrefix("--cutoff-days=").toInt }
}
```

- [ ] **Step 2:** Rebuild

```bash
cd final
docker compose build spark-job
docker compose up -d spark-job
```

- [ ] **Step 3:** Bootstrap aggregation (full mode — covers historical 2020 data)

```bash
docker compose exec spark-job spark-submit \
  --class ru.consultant.lakehouse.jobs.AggregateJob \
  /app/jars/spark-jobs.jar --mode=full
```

Expected last line: `[AggregateJob] done`.

- [ ] **Step 4:** Read metric 1 (`ACC_45616` hits)

```bash
docker compose exec spark-job spark-sql -e \
  "SELECT sum(hits) AS total_hits FROM warehouse.gold.card_search_doc_hits_daily WHERE doc_id = 'ACC_45616'"
```

Expected: single non-zero number.

- [ ] **Step 5:** Read metric 2 (top 20 daily opens via QS)

```bash
docker compose exec spark-job spark-sql -e \
  "SELECT open_date, doc_id, opens FROM warehouse.gold.qs_doc_opens_daily ORDER BY opens DESC LIMIT 20"
```

Expected: ~20 rows with `(date, doc_id, opens)`.

- [ ] **Step 6:** Verify idempotency

```bash
docker compose exec spark-job spark-submit \
  --class ru.consultant.lakehouse.jobs.AggregateJob \
  /app/jars/spark-jobs.jar --mode=full
```

Re-check metric 1 — same number.

- [ ] **Step 7:** Commit

```bash
git add final/spark-jobs/src/main/scala
git commit -m "feat(spark): AggregateJob with MERGE INTO gold tables (full + incremental modes)"
```

---

## Phase 6 — Rust generator

Goal: a long-running container that snapshots distributions from historical sessions on first start, then loops to PUT new synthetic sessions into `s3://sessions/` every N seconds with `event_time = now()`.

### Task 6.1: `generator/Cargo.toml`

**Files:**
- Create: `final/generator/Cargo.toml`

- [ ] **Step 1:** Write file

```toml
[package]
name = "generator"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio          = { version = "1", features = ["full"] }
aws-config     = { version = "1", features = ["behavior-version-latest"] }
aws-sdk-s3     = "1"
aws-credential-types = "1"
rand           = "0.8"
rand_distr     = "0.4"
encoding_rs    = "0.8"
uuid           = { version = "1", features = ["v4"] }
serde          = { version = "1", features = ["derive"] }
serde_json     = "1"
chrono         = "0.4"
anyhow         = "1"
tracing        = "0.1"
tracing-subscriber = "0.3"

[profile.release]
opt-level = 3
lto = "fat"
codegen-units = 1
strip = true
```

### Task 6.2: `generator/src/main.rs` — skeleton (config + S3 client)

**Files:**
- Create: `final/generator/src/main.rs`

- [ ] **Step 1:** Write skeleton

```rust
use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use std::env;
use std::time::Duration;
use tokio::time;
use tracing_subscriber::EnvFilter;

mod dist;
mod session;
mod s3io;

#[derive(Debug, Clone)]
struct AppConfig {
    s3_endpoint:       String,
    s3_bucket:         String,
    s3_region:         String,
    aws_access_key:    String,
    aws_secret_key:    String,
    interval_seconds:  u64,
    dist_cache_path:   String,
}

impl AppConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            s3_endpoint:       env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://minio:9000".into()),
            s3_bucket:         env::var("S3_BUCKET").unwrap_or_else(|_| "sessions".into()),
            s3_region:         env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into()),
            aws_access_key:    env::var("AWS_ACCESS_KEY_ID").context("AWS_ACCESS_KEY_ID")?,
            aws_secret_key:    env::var("AWS_SECRET_ACCESS_KEY").context("AWS_SECRET_ACCESS_KEY")?,
            interval_seconds:  env::var("INTERVAL_SECONDS").unwrap_or_else(|_| "5".into()).parse()?,
            dist_cache_path:   env::var("DIST_CACHE_PATH").unwrap_or_else(|_| "/var/cache/generator/distributions.json".into()),
        })
    }
}

async fn build_s3(cfg: &AppConfig) -> Result<aws_sdk_s3::Client> {
    let creds = Credentials::new(
        &cfg.aws_access_key,
        &cfg.aws_secret_key,
        None, None, "static",
    );
    let conf = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(cfg.s3_region.clone()))
        .endpoint_url(&cfg.s3_endpoint)
        .credentials_provider(creds)
        .load()
        .await;
    let s3_conf = aws_sdk_s3::config::Builder::from(&conf)
        .force_path_style(true)
        .build();
    Ok(aws_sdk_s3::Client::from_conf(s3_conf))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let cfg = AppConfig::from_env()?;
    tracing::info!(?cfg, "starting generator");

    let s3 = build_s3(&cfg).await?;

    // Load or snapshot distributions
    let dist = dist::load_or_snapshot(&s3, &cfg).await?;
    tracing::info!("distributions ready: {} sessions sampled", dist.sample_size);

    let mut interval = time::interval(Duration::from_secs(cfg.interval_seconds));
    loop {
        interval.tick().await;
        match session::generate_and_upload(&s3, &cfg, &dist).await {
            Ok(key) => tracing::info!(key, "uploaded synthetic session"),
            Err(e)  => tracing::error!(error = %e, "generation failed"),
        }
    }
}
```

### Task 6.3: `generator/src/dist.rs` — distribution snapshot + cache

**Files:**
- Create: `final/generator/src/dist.rs`

- [ ] **Step 1:** Write file

```rust
use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

use crate::AppConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Distributions {
    pub sample_size: usize,
    /// frequencies of event types: SESSION_START is implicit. counts events PER session.
    pub events_per_session: Vec<u32>,   // empirical samples of "n events between START and END"
    pub event_kinds: HashMap<String, f64>,     // weight of each kind: "QS","CARD","DOC_OPEN"
    pub inter_event_seconds: Vec<u32>,         // empirical gaps in seconds
    pub doc_ids: Vec<(String, u64)>,           // (doc_id, freq) — for sampling result lists
    pub queries: Vec<String>,                  // a small sample of query texts
}

pub async fn load_or_snapshot(s3: &S3Client, cfg: &AppConfig) -> Result<Distributions> {
    let path = Path::new(&cfg.dist_cache_path);
    if path.exists() {
        let txt = std::fs::read_to_string(path)?;
        let d: Distributions = serde_json::from_str(&txt)?;
        tracing::info!("loaded cached distributions");
        return Ok(d);
    }
    tracing::info!("snapshotting distributions from s3://{}/", cfg.s3_bucket);
    let d = snapshot_from_s3(s3, cfg).await?;
    if let Some(parent) = path.parent() { std::fs::create_dir_all(parent).ok(); }
    std::fs::write(path, serde_json::to_string(&d)?)?;
    Ok(d)
}

async fn snapshot_from_s3(s3: &S3Client, cfg: &AppConfig) -> Result<Distributions> {
    // List up to ~10k objects under sessions/ and sample N of them
    let mut continuation: Option<String> = None;
    let mut all_keys: Vec<String> = Vec::new();
    loop {
        let mut req = s3.list_objects_v2().bucket(&cfg.s3_bucket).max_keys(1000);
        if let Some(t) = &continuation { req = req.continuation_token(t); }
        let resp = req.send().await.context("list_objects_v2")?;
        if let Some(contents) = resp.contents {
            for o in contents { if let Some(k) = o.key { all_keys.push(k); } }
        }
        if resp.is_truncated.unwrap_or(false) {
            continuation = resp.next_continuation_token;
        } else { break; }
    }
    tracing::info!("found {} keys for distribution snapshot", all_keys.len());

    // To keep startup fast, sample only first ~500
    let sample_n = all_keys.len().min(500);
    let mut events_per_session: Vec<u32>     = Vec::new();
    let mut event_kinds:        HashMap<String,u64> = HashMap::new();
    let mut inter_event_seconds: Vec<u32>    = Vec::new();
    let mut doc_id_freq:        HashMap<String,u64> = HashMap::new();
    let mut queries: Vec<String>             = Vec::new();

    for key in all_keys.iter().take(sample_n) {
        let obj = s3.get_object().bucket(&cfg.s3_bucket).key(key).send().await?;
        let body = obj.body.collect().await?.into_bytes();
        let (cow, _, _) = encoding_rs::WINDOWS_1251.decode(&body);
        let text = cow.into_owned();
        let stats = crate::session::scan_session(&text);
        events_per_session.push(stats.event_count);
        inter_event_seconds.extend(stats.gaps);
        for k in stats.kinds.iter() { *event_kinds.entry(k.clone()).or_insert(0) += 1; }
        for d in stats.docs.iter()  { *doc_id_freq.entry(d.clone()).or_insert(0) += 1; }
        queries.extend(stats.queries);
    }

    let total_kinds: u64 = event_kinds.values().sum();
    let event_kinds_norm: HashMap<String, f64> = event_kinds.into_iter()
        .map(|(k,v)| (k, v as f64 / total_kinds.max(1) as f64))
        .collect();

    // Take top-N doc_ids by frequency to keep cache size small
    let mut docs_vec: Vec<(String,u64)> = doc_id_freq.into_iter().collect();
    docs_vec.sort_by(|a,b| b.1.cmp(&a.1));
    docs_vec.truncate(5000);

    queries.truncate(200);

    Ok(Distributions {
        sample_size:        sample_n,
        events_per_session,
        event_kinds:        event_kinds_norm,
        inter_event_seconds,
        doc_ids:            docs_vec,
        queries,
    })
}
```

### Task 6.4: `generator/src/session.rs` — session sampling + serialization

**Files:**
- Create: `final/generator/src/session.rs`

- [ ] **Step 1:** Write file

```rust
use anyhow::Result;
use aws_sdk_s3::Client as S3Client;
use chrono::{Utc, Datelike, Timelike, Duration};
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use rand_distr::WeightedIndex;
use rand_distr::Distribution as RandDistribution;
use uuid::Uuid;

use crate::{AppConfig, dist::Distributions};

/// Stats extracted from a real session — used to build distributions
pub struct SessionStats {
    pub event_count: u32,
    pub gaps:        Vec<u32>,
    pub kinds:       Vec<String>,
    pub docs:        Vec<String>,
    pub queries:     Vec<String>,
}

/// Best-effort scan of a session text to extract structural stats for distribution building.
pub fn scan_session(text: &str) -> SessionStats {
    let mut stats = SessionStats {
        event_count: 0, gaps: vec![], kinds: vec![], docs: vec![], queries: vec![],
    };
    let mut last_ts: Option<chrono::DateTime<Utc>> = None;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() { continue; }
        let ts_opt = extract_ts(line);
        if let (Some(t), Some(prev)) = (ts_opt, last_ts) {
            let gap = (t - prev).num_seconds().max(0) as u32;
            if gap < 3600 { stats.gaps.push(gap); } // ignore obvious outliers
        }
        if let Some(t) = ts_opt { last_ts = Some(t); }

        if line.starts_with("QS ") {
            stats.kinds.push("QS".into());
            stats.event_count += 1;
            if let Some(q) = extract_query(line) { stats.queries.push(q); }
        } else if line.starts_with("CARD_SEARCH_START") {
            stats.kinds.push("CARD".into());
            stats.event_count += 1;
        } else if line.starts_with("DOC_OPEN ") {
            stats.kinds.push("DOC_OPEN".into());
            stats.event_count += 1;
            if let Some(doc) = line.split_whitespace().last() {
                stats.docs.push(doc.to_string());
            }
        } else if line.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false) {
            // results line: "<id> doc1 doc2 ..."
            for token in line.split_whitespace().skip(1) {
                if token.contains('_') { stats.docs.push(token.to_string()); }
            }
        }
    }
    stats
}

fn extract_ts(line: &str) -> Option<chrono::DateTime<Utc>> {
    // try "dd.MM.yyyy_HH:mm:ss" pattern at position after the prefix
    let tokens: Vec<&str> = line.split_whitespace().collect();
    if tokens.len() < 2 { return None; }
    let ts_token = tokens[1];
    chrono::NaiveDateTime::parse_from_str(ts_token, "%d.%m.%Y_%H:%M:%S").ok()
        .map(|nd| chrono::DateTime::<Utc>::from_naive_utc_and_offset(nd, Utc))
}

fn extract_query(line: &str) -> Option<String> {
    let start = line.find('{')?;
    let end = line.rfind('}')?;
    if end > start { Some(line[start+1..end].to_string()) } else { None }
}

/// Generate one synthetic session, encode cp1251, PUT into s3://{bucket}/<uuid>.txt
pub async fn generate_and_upload(s3: &S3Client, cfg: &AppConfig, dist: &Distributions) -> Result<String> {
    let body_utf8 = render_session(dist);
    let (encoded, _, _) = encoding_rs::WINDOWS_1251.encode(&body_utf8);

    let key = format!("synthetic-{}.txt", Uuid::new_v4());
    s3.put_object()
        .bucket(&cfg.s3_bucket)
        .key(&key)
        .body(aws_sdk_s3::primitives::ByteStream::from(encoded.into_owned()))
        .send().await?;
    Ok(key)
}

fn render_session(d: &Distributions) -> String {
    let mut rng = thread_rng();

    // # of "main events" (QS / CARD / DOC_OPEN) in this session
    let n_events: u32 = *d.events_per_session
        .choose(&mut rng)
        .unwrap_or(&5);
    let n_events = n_events.max(1).min(50);

    // Pick kinds via weighted distribution
    let kinds: Vec<&String> = d.event_kinds.keys().collect();
    let weights: Vec<f64> = kinds.iter().map(|k| *d.event_kinds.get(*k).unwrap()).collect();
    let kind_dist = WeightedIndex::new(&weights).unwrap();

    // Start session time = now
    let mut t = Utc::now();
    let mut out = String::new();
    out.push_str(&format!("SESSION_START {}\n", fmt_ts(t)));

    // We need to track current search_id for DOC_OPEN to reference
    let mut last_search_id: Option<String> = None;

    for _ in 0..n_events {
        // advance time by a sampled gap
        let gap = *d.inter_event_seconds.choose(&mut rng).unwrap_or(&5);
        t = t + Duration::seconds(gap as i64);

        let kind = kinds[kind_dist.sample(&mut rng)];
        match kind.as_str() {
            "QS" => {
                let q = d.queries.choose(&mut rng).cloned().unwrap_or_else(|| "test".into());
                let sid = format!("{}", rng.gen_range(1_000_000u64..999_999_999u64));
                last_search_id = Some(sid.clone());
                out.push_str(&format!("QS {} {{{}}}\n", fmt_ts(t), q));
                let docs = sample_docs(d, &mut rng, rng.gen_range(1..20));
                out.push_str(&format!("{} {}\n", sid, docs.join(" ")));
            }
            "CARD" => {
                let sid = format!("{}", rng.gen_range(1_000_000u64..999_999_999u64));
                last_search_id = Some(sid.clone());
                out.push_str(&format!("CARD_SEARCH_START {}\n", fmt_ts(t)));
                let n_params = rng.gen_range(1..=2);
                for _ in 0..n_params {
                    let pid = rng.gen_range(0..200);
                    let pval = d.doc_ids.choose(&mut rng).map(|x| x.0.clone()).unwrap_or_default();
                    out.push_str(&format!("${} {}\n", pid, pval));
                }
                out.push_str("CARD_SEARCH_END\n");
                let docs = sample_docs(d, &mut rng, rng.gen_range(1..30));
                out.push_str(&format!("{} {}\n", sid, docs.join(" ")));
            }
            "DOC_OPEN" => {
                if let Some(sid) = &last_search_id {
                    let doc = sample_one_doc(d, &mut rng);
                    out.push_str(&format!("DOC_OPEN {} {} {}\n", fmt_ts(t), sid, doc));
                }
            }
            _ => {}
        }
    }

    t = t + Duration::seconds(rng.gen_range(1..=15));
    out.push_str(&format!("SESSION_END {}\n", fmt_ts(t)));
    out
}

fn sample_docs<R: Rng>(d: &Distributions, rng: &mut R, n: usize) -> Vec<String> {
    (0..n).map(|_| sample_one_doc(d, rng)).collect()
}

fn sample_one_doc<R: Rng>(d: &Distributions, rng: &mut R) -> String {
    d.doc_ids.choose(rng).map(|x| x.0.clone()).unwrap_or_else(|| "LAW_0".into())
}

fn fmt_ts(t: chrono::DateTime<Utc>) -> String {
    format!(
        "{:02}.{:02}.{:04}_{:02}:{:02}:{:02}",
        t.day(), t.month(), t.year(), t.hour(), t.minute(), t.second()
    )
}
```

### Task 6.5: `generator/src/s3io.rs` — placeholder for future

**Files:**
- Create: `final/generator/src/s3io.rs`

- [ ] **Step 1:** Write file

```rust
// Reserved for future S3 helpers (cached listings, etc.)
```

### Task 6.6: `generator/Dockerfile`

**Files:**
- Create: `final/generator/Dockerfile`

- [ ] **Step 1:** Write file

```dockerfile
# ---------- stage 1: build ----------
FROM rust:1-slim AS build
WORKDIR /src
COPY Cargo.toml ./
# pre-fetch deps with a stub main
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo build --release && rm -rf src target/release/generator
COPY src ./src
RUN touch src/main.rs && cargo build --release

# ---------- stage 2: runtime ----------
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
RUN useradd -m -u 1001 app && mkdir -p /var/cache/generator && chown -R app /var/cache/generator
USER app
COPY --from=build /src/target/release/generator /usr/local/bin/generator
ENTRYPOINT ["/usr/local/bin/generator"]
```

### Task 6.7: Add `generator` service to compose

**Files:**
- Modify: `final/docker-compose.yml`

- [ ] **Step 1:** Add generator service (append in `services:` section)

```yaml
  generator:
    build:
      context: ./generator
      dockerfile: Dockerfile
    image: cs-lakehouse/generator:latest
    container_name: generator
    networks: [lakehouse-net]
    depends_on:
      mc-init: { condition: service_completed_successfully }
    env_file: [.env]
    environment:
      S3_ENDPOINT: http://minio:9000
      S3_BUCKET: sessions
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
      AWS_REGION: ${AWS_REGION}
      INTERVAL_SECONDS: ${GENERATOR_INTERVAL_SECONDS}
      DIST_CACHE_PATH: /var/cache/generator/distributions.json
      RUST_LOG: info
    restart: unless-stopped
```

### Task 6.8: Build + run generator

- [ ] **Step 1:** Build

```bash
cd final
docker compose build generator
```

Expected: build completes in 3-8 min on first run (cargo dep compile).

- [ ] **Step 2:** Run

```bash
docker compose up -d generator
sleep 10
docker compose logs generator | tail -20
```

Expected: lines like `"snapshotting distributions from s3://sessions/"`, then `"distributions ready"`, then periodic `"uploaded synthetic session ... key=synthetic-<uuid>.txt"`.

- [ ] **Step 3:** Verify new files in MinIO

Open MinIO UI → bucket `sessions/` → see `synthetic-*.txt` files appearing every N seconds.

- [ ] **Step 4:** Verify one synthetic file is parseable by Spark

```bash
# wait ~30 sec for several files
docker compose exec spark-job spark-submit \
  --class ru.consultant.lakehouse.jobs.ParseJob \
  /app/jars/spark-jobs.jar
```

Expected last line: `discovered 1000X total, X new` where X is the number of synthetic files generated so far.

- [ ] **Step 5:** Commit

```bash
git add final/generator final/docker-compose.yml
git commit -m "feat(generator): Rust service that snapshots dists + uploads synthetic sessions"
```

---

## Phase 7 — Scheduler (ofelia)

Goal: every 30 sec, ofelia execs `ParseJob` then `AggregateJob` inside the `spark-job` container. Manual bootstrap is documented separately.

### Task 7.1: `scheduler/config.ini`

**Files:**
- Create: `final/scheduler/config.ini`

- [ ] **Step 1:** Write file

```ini
[global]
save-folder = /var/log/ofelia

[job-exec "parse"]
schedule = @every 30s
container = spark-job
command = spark-submit --class ru.consultant.lakehouse.jobs.ParseJob /app/jars/spark-jobs.jar

[job-exec "aggregate"]
schedule = @every 30s
container = spark-job
command = spark-submit --class ru.consultant.lakehouse.jobs.AggregateJob /app/jars/spark-jobs.jar --mode=incremental
```

### Task 7.2: Add `scheduler` service to compose

**Files:**
- Modify: `final/docker-compose.yml`

- [ ] **Step 1:** Add service

```yaml
  scheduler:
    image: mcuadros/ofelia:latest
    container_name: scheduler
    networks: [lakehouse-net]
    depends_on:
      spark-job: { condition: service_started }
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - ./scheduler/config.ini:/etc/ofelia/config.ini:ro
    command: daemon --config=/etc/ofelia/config.ini
    restart: unless-stopped
```

### Task 7.3: End-to-end live test

- [ ] **Step 1:** Tear down and rebuild from scratch

```bash
cd final
docker compose down -v
docker compose up -d --build
```

Wait ~60 sec for everything to settle.

- [ ] **Step 2:** Manual bootstrap (one-time)

```bash
docker compose exec spark-job spark-submit \
  --class ru.consultant.lakehouse.jobs.ParseJob \
  /app/jars/spark-jobs.jar

docker compose exec spark-job spark-submit \
  --class ru.consultant.lakehouse.jobs.AggregateJob \
  /app/jars/spark-jobs.jar --mode=full
```

Both should complete successfully.

- [ ] **Step 3:** Capture baseline counts

```bash
docker compose exec spark-job spark-sql -e \
  "SELECT count(*) FROM warehouse.bronze.events;"
# Note this number, call it E0

docker compose exec spark-job spark-sql -e \
  "SELECT sum(hits) FROM warehouse.gold.card_search_doc_hits_daily WHERE doc_id='ACC_45616';"
# This is the target metric 1 number for the historical 10k corpus
```

- [ ] **Step 4:** Wait 3 minutes — generator adds files, scheduler runs ParseJob + AggregateJob every 30 sec

```bash
sleep 180
docker compose logs scheduler | tail -30
```

Expected: see `job parse finished` and `job aggregate finished` lines every ~30 sec, with `exit_status 0`.

- [ ] **Step 5:** Recount

```bash
docker compose exec spark-job spark-sql -e \
  "SELECT count(*) FROM warehouse.bronze.events;"
# Should be > E0

docker compose exec spark-job spark-sql -e \
  "SELECT open_date, sum(opens) FROM warehouse.gold.qs_doc_opens_daily WHERE open_date >= current_date() - INTERVAL 1 DAY GROUP BY open_date ORDER BY open_date;"
# Should show today's data growing from synthetic sessions
```

- [ ] **Step 6:** Commit

```bash
git add final/scheduler final/docker-compose.yml
git commit -m "feat(scheduler): ofelia cron driving ParseJob + AggregateJob every 30s"
```

---

## Phase 8 — Project README + acceptance documentation

Goal: a `README.md` that a fresh evaluator can follow to reproduce the metric numbers from `task.md`.

### Task 8.1: `final/README.md`

**Files:**
- Create: `final/README.md`

- [ ] **Step 1:** Write README

````markdown
# Spark Lakehouse Pipeline — Финальный проект

Полностью контейнеризованный pipeline: парсит логи сессий КонсультантПлюс в Iceberg-таблицы
поверх MinIO, считает два целевых KPI задания, поддерживает «живые» данные через
Rust-генератор и периодические Spark-джобы.

См. дизайн: `docs/superpowers/specs/2026-05-23-spark-lakehouse-design.md`

## Требования

- Docker 24+ и Docker Compose v2
- Свободные порты: 9000, 9001, 19120
- ~3-4 GB free disk и ~4 GB RAM для контейнеров

## Запуск с нуля

```bash
cd final
docker compose up -d --build
# подождать ~1 мин: mc-init загружает 10k файлов, поднимаются minio/nessie/spark/generator/scheduler
```

## Bootstrap (один раз — пересчёт по всей истории)

```bash
docker compose exec spark-job spark-submit \
  --class ru.consultant.lakehouse.jobs.ParseJob \
  /app/jars/spark-jobs.jar

docker compose exec spark-job spark-submit \
  --class ru.consultant.lakehouse.jobs.AggregateJob \
  /app/jars/spark-jobs.jar --mode=full
```

После этого scheduler автоматически пересчитывает gold каждые 30 сек, генератор подсыпает новые
сессии каждые 5 сек.

## Результаты задания

### Метрика 1: количество карточных поисков, вернувших ACC_45616

```bash
docker compose exec spark-job spark-sql -e \
  "SELECT sum(hits) AS total FROM warehouse.gold.card_search_doc_hits_daily WHERE doc_id = 'ACC_45616';"
```

### Метрика 2: открытия документов через QS за каждый день

```bash
docker compose exec spark-job spark-sql -e \
  "SELECT open_date, doc_id, opens
   FROM warehouse.gold.qs_doc_opens_daily
   ORDER BY open_date, opens DESC
   LIMIT 100;"
```

Для выгрузки в CSV:

```bash
docker compose exec spark-job spark-sql -e \
  "SELECT open_date, doc_id, opens FROM warehouse.gold.qs_doc_opens_daily ORDER BY open_date" \
  > metric2.csv
```

## Сервисы

| URL                             | Что |
|---------------------------------|-----|
| http://localhost:9001           | MinIO Console (admin / admin12345) |
| http://localhost:19120/tree/main| Nessie UI — Iceberg-таблицы и история snapshots |

## Структура проекта

См. `docs/superpowers/specs/2026-05-23-spark-lakehouse-design.md`, section 7.

## Тонкости

- Все сервисы в Docker — Spark/Scala/Maven устанавливать на хост **не нужно**.
- Кодировка файлов сессий — **cp1251**, ParseJob и генератор обрабатывают её корректно.
- Hot-window cutoff = 3 дня. Сценарий «AggregateJob простаивал > 3 дней» приводит к потере
  промежуточных дней; для дипломки полагаемся на непрерывную работу scheduler'а
  (см. design doc, section 4.1).
````

- [ ] **Step 2:** Commit

```bash
git add final/README.md
git commit -m "docs: README with run + acceptance instructions"
```

---

## Self-Review (planning author's check)

**Spec coverage:**
- §3.1 топология сервисов → Phases 1, 2, 6, 7 (minio/mc-init/nessie/spark-job/generator/scheduler)
- §3.2 слои данных → Phase 4 (bronze schemas), Phase 5 (gold schemas)
- §3.3 pipeline-фазы → Phase 4 (ParseJob), Phase 5 (AggregateJob), Phase 7 (scheduler), Phase 8 (bootstrap docs)
- §4.1 hot-window-recompute + допущение → AggregateJob CLI flags, README mentions assumption
- §4.2 bootstrap → Phase 7 task 7.3 step 2
- §4.3 всё в Docker → Phase 2 Dockerfile + compose (no host Spark)
- §4.4 Iceberg lakehouse → Phase 4-5 use Nessie catalog + Iceberg MERGE
- §4.5 pure-Scala парсер → Phase 3 (parser/ has no Spark imports)
- §5 схемы → Phase 4.1 (IcebergSchemas.scala)
- §6.1 ParseJob алгоритм → Phase 4.5
- §6.2 AggregateJob алгоритм → Phase 5.2
- §7 структура проекта → File Structure section + Phases match it
- §8 compose → Phases 1, 2, 6, 7 wire all services
- §9 generator → Phase 6
- §10 open questions: CLI parsing (5.2 implements manual), iceberg-aws-bundle (2.1 deps), Java 17 (2.1, 2.5), dep prewarm (2.5 best-effort), cp1251 UTC (covered), at-least-once ParseJob (4.5 implements dedup)
- §11 acceptance → Phase 7.3 (end-to-end test) + README

**Placeholder scan:** no TBD/TODO/"implement later" in actionable steps. (Comments inside code like "// Reserved for future" are acceptable file-level stubs.)

**Type consistency:** verified —
- `RawEvent` smart constructors match what `SessionParser` calls (sessionStart/sessionEnd/qs/cardSearch/docOpen/malformed)
- `RawEvent.docOpen` takes `resolvedKind: Option[String]` and `SessionParser` passes `kindBySearchId.get(sid)` — matches
- `BronzeRowSchema` columns match table DDL in `IcebergSchemas.ensureBronzeEvents`
- `GoldWriter` table names match `IcebergSchemas.ensureGoldCardHits`/`ensureGoldQsOpens` and `AppConfig`
- `AppConfig.cutoffDays` is Int; `AggregateJob` reads it as Int; `LocalDate.minusDays(Long)` — cast applied
- `ParseJob` calls `BronzeWriter.appendEvents(spark, df)` and `BronzeWriter.markProcessed(spark, newPaths)` — signatures match
