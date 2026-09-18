# Built from the repo root (see compose.yaml) so the tests can reach internal/wire/testdata.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src/flink
COPY internal/wire/testdata /src/internal/wire/testdata
COPY flink/pom.xml .
COPY flink/src src
# the cache mount keeps ~/.m2 across builds, plugins included, which dependency:go-offline doesn't fully cover
RUN --mount=type=cache,target=/root/.m2 mvn -q -B package

FROM flink:2.2.1-java17
COPY --from=build /src/flink/target/processor.jar /opt/flink/usrlib/processor.jar
# created as flink (the image's user) so the checkpoints volume inherits the ownership when compose first creates it
RUN mkdir /opt/flink/checkpoints
