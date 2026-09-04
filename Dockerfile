# Multi-stage build shared by coordinator and worker images.
# Build:  docker build --target coordinator -t dtp-coordinator .
#         docker build --target worker -t dtp-worker .
# Or just: docker compose up --build

FROM eclipse-temurin:21-jdk AS build
WORKDIR /build
COPY mvnw pom.xml ./
COPY .mvn .mvn
COPY common/pom.xml common/pom.xml
COPY coordinator/pom.xml coordinator/pom.xml
COPY worker/pom.xml worker/pom.xml
COPY client/pom.xml client/pom.xml
COPY integration-tests/pom.xml integration-tests/pom.xml
# Warm the dependency cache before copying sources so code changes don't
# re-download the world.
RUN ./mvnw -q -B dependency:go-offline -pl common,coordinator,worker,client || true
COPY common common
COPY coordinator coordinator
COPY worker worker
COPY client client
RUN ./mvnw -q -B package -DskipTests -pl common,coordinator,worker,client

FROM eclipse-temurin:21-jre AS coordinator
WORKDIR /app
COPY --from=build /build/coordinator/target/dtp-coordinator-*.jar app.jar
EXPOSE 7070 7071
ENTRYPOINT ["java", "-jar", "app.jar"]

FROM eclipse-temurin:21-jre AS worker
WORKDIR /app
COPY --from=build /build/worker/target/dtp-worker-*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
