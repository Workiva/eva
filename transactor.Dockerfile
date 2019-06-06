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
COPY target/transactor.jar /eva-server/
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
