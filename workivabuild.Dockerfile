FROM clojure:openjdk-8-lein-2.8.3-alpine as build

# Allow snapshot dependencies
ENV LEIN_SNAPSHOTS_IN_RELEASE=true

# Get Git and Maven
RUN apk add --update git maven

# Copy local compose files to make and make them a smithy artifact for local consumption
COPY ./docker/docker-compose.yml /compose/local-compose-eva.yml
ARG BUILD_ARTIFACTS_COMPOSE=/compose/local-compose-eva.yml

# Copy project.clj over and fetch deps so they're cached in a layer
# Also need to include credentials before we can pull down dependencies
WORKDIR /build
COPY ./project.clj /build/project.clj
RUN lein deps

# Copy over project for build
COPY . /build

# Cache Java Deps / Credentials
ENV MAVEN_OPTS="-Xmx4096m"

# Run Build
# Used to Speed up Local Builds
ARG SKIP_TESTS
RUN echo "Running Build" \
 && ./scripts/ci/workiva-build.sh \
 && echo "Build Complete"

# Artifacts
ARG BUILD_ARTIFACTS_DOCUMENTATION=/build/eva-api-docs.tgz
ARG BUILD_ARTIFACTS_ARTIFACTORY=/build/eva-*.jar

# Prepare Veracode Artifact (Transactor & Client Library)
FROM clojure:openjdk-8-lein-2.8.3-alpine AS veracode

# Allow snapshot dependencies
ENV LEIN_SNAPSHOTS_IN_RELEASE=true

WORKDIR /build
COPY --from=build /build /build
COPY --from=build /root/.m2 /root/.m2
RUN mkdir ./veracode

## Transactor
RUN lein with-profile +deployment,+debug-compile uberjar
RUN cd ./target && tar czf ../veracode/java.tar.gz ./transactor.jar

## Artifacts
ARG BUILD_ARTIFACTS_VERACODE=/build/veracode/java.tar.gz

# Prepare Final Image
FROM openjdk:8u181-jre-alpine3.8

## Setup Environment
### Alpine does not come with bash by default
### As well as some required c libraries for yourkit https://www.yourkit.com/docs/java/help/docker.jsp
RUN apk add --update bash curl libc6-compat nss

ENV EVA_SERVER_STATUS_HTTP_PORT 9999

EXPOSE 5445/tcp
EXPOSE 9999/tcp

## Copy in Artifact
WORKDIR /eva-server
RUN chown root:root /eva-server
COPY --from=build /build/transactor.jar /eva-server/
RUN chmod -R 755 /eva-server

## Copy in Scripts
COPY ./scripts/image/set_mem_constraints.sh /usr/local/bin/set_mem_constraints.sh
COPY ./scripts/image/run_service.sh /usr/local/bin/run_service.sh
COPY server/test-resources/eva/server/v2/local_transactors_test_config.clj /local_transactors_test_config.clj
COPY server/test-resources/eva/server/v2/local.xml /local_logback.xml

### Ensure scripts are executable
RUN chmod +x /usr/local/bin/set_mem_constraints.sh
RUN chmod +x /usr/local/bin/run_service.sh

### Update Packages for latest Security Updates
ARG BUILD_ID
RUN apk update && apk upgrade
USER nobody
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 CMD [ "curl", "--fail" "http://localhost:9999/status", "||", "exit", "1" ]
CMD [ "sh", "/usr/local/bin/run_service.sh" ]
