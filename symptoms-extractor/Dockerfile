# STAGE 1
# cache as much as possible in this multistage Dockerfile.
FROM maven:3.8.1-jdk-17 as dependies

WORKDIR /opt/app

COPY parent-poms/*.xml parent-poms/
COPY common/pom.xml common/pom.xml
COPY orchestrator/pom.xml orchestrator/pom.xml
COPY normalizer/pom.xml normalizer/pom.xml
COPY diagnoser/pom.xml diagnoser/pom.xml
COPY symptoms-extractor/pom.xml symptoms-extractor/pom.xml
COPY planner/pom.xml planner/pom.xml
COPY pom.xml .

# install any required custom libraries if any
# RUN mvn install:install-file
# RUN mvn install:install-file

RUN mvn -B -e -C org.apache.maven.plugins:maven-dependency-plugin:3.2.0:go-offline

# STAGE 2
# Copy the dependencies from the dependies stage with the advantage of using docker layer caches.
# If something goes wrong from this line on, all dependencies from dependies were already downloaded and stored in docker's layers.
FROM eclipse-temurin:24-jdk as builder

WORKDIR /opt/app

COPY --from=dependies /root/.m2 /root/.m2
COPY --from=dependies /opt/app/ /opt/app

COPY common/src common/src
COPY symptoms-extractor/src symptoms-extractor/src

RUN mvn -B -e clean install -DskipTests=true

# STAGE 3
# At this point, builder stage should have the symptoms-extractor .jar file
FROM eclipse-temurin:17-jre-alpine

WORKDIR /opt/app

RUN mkdir logs && chmod 777 logs
RUN adduser -S symptomsextractorappuser -g symptomsextractorappuser
RUN apk add --upgrade expat

ARG APP_VERSION=*
ARG APP_JAR_FILE_NAME=symptoms-extractor-$APP_VERSION.jar

COPY --from=builder /opt/app/symptoms-extractor/target/${APP_JAR_FILE_NAME} .

ENTRYPOINT ["java", "-Xms512m","-Xmx1G","-jar","/opt/app/${APP_JAR_FILE_NAME}.jar"]

USER symptomsextractorappuser
