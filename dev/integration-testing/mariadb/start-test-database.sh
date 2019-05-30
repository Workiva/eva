#!/usr/bin/env bash

set -e
[ -n "$DEBUG" ] && set -x

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
    DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

PROJECT_DIR="$(cd -P "$DIR/../../.." && pwd)"
cd "$DIR"

hash docker 2>/dev/null || { echo >&2 "docker command not found.  Aborting."; exit 1; }

docker ps >/dev/null

MARIADB_CONTAINER_IMAGE=${MARIADB_CONTAINER_IMAGE:-"mariadb:10.1"}
MARIADB_CONTAINER_NAME=${MARIADB_CONTAINER_NAME:-"eva-mariadb-test-db"}
MARIADB_PORT=${MARIADB_PORT:-3306}

mariadb_started=0

function stop_mariadb() {
  if [ $mariadb_started = 1 ]; then
    local container_name=$(docker rm -f "$MARIADB_CONTAINER_NAME")
    echo "Shutdown container: $container_name"
  fi
}
trap stop_mariadb EXIT

echo "Starting MariaDB Container..."
# Start MariaDB Container
container_id="$(docker run --name "$MARIADB_CONTAINER_NAME" \
  -e MYSQL_RANDOM_ROOT_PASSWORD=1 \
  -e MYSQL_DATABASE=eva \
  -e MYSQL_USER=eva \
  -e MYSQL_PASSWORD=notasecret \
  -p "3306:$MARIADB_PORT" \
  -d \
  "$MARIADB_CONTAINER_IMAGE")"
mariadb_started=1


docker cp "$PROJECT_DIR/core/resources/eva/storage/sql/mariadb/mariadb-table.sql" "$MARIADB_CONTAINER_NAME:/mariadb-table.sql"
docker exec "$MARIADB_CONTAINER_NAME" bin/bash -c 'while [ "$(pgrep mysqld)" -ne 1 ]; do sleep 1; done; sleep 5; mysql  -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" < /mariadb-table.sql'

if [ ! -z "$DOCKER_HOST" ]; then
  MARIADB_HOST=$(echo $DOCKER_HOST | awk -F/ '{print $3}' | awk -F: '{print $1}')
else
  MARIADB_HOST="localhost"
fi

jdbc_uri="jdbc:mariadb://$MARIADB_HOST:$MARIADB_PORT/eva?user=eva&password=notasecret"
eva_uri="eva:sql://<NAMESPACE>?${jdbc_uri}"

echo ""
echo "Started MariaDB:"
echo "- container-name: $MARIADB_CONTAINER_NAME"
echo "- container-id:   $container_id"
echo "- db-host:        $MARIADB_HOST"
echo "- db-port:        $MARIADB_PORT"
echo "- db-username:    eva"
echo "- db-password:    notasecret"
echo ""
echo "Initialized Eva SQL-storage database and table."
echo ""
echo ""
echo "Maria JDBC URI: ${jdbc_uri}"
echo ""
echo "Eva storage-uri: "
echo "  $eva_uri"
echo ""
echo "To run tests against this server, execute command:"
echo "  $ EVA_TEST_DB_URI_TEMPLATE='$eva_uri' lein test"
echo ""
echo "Type Ctrl+c to exit stop MariaDB server..."
docker wait "$MARIADB_CONTAINER_NAME"

