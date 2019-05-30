#!/usr/bin/env bash

# Abort script on any error
set -e

if [ -f /usr/local/bin/set_mem_constraints.sh ]; then
  source /usr/local/bin/set_mem_constraints.sh
else
  source "$(dirname "$0")"/set_mem_constraints.sh
fi


if [ -n $LOGBACK_CONFIG ] ; then
   JAVA_OPTS="${JAVA_OPTS} -Dlogback.configurationFile=${LOGBACK_CONFIG}"
fi

## Support for enabling the YourKit Profiling Agent
yourkit_options=""
YOURKIT_AGENT_PATH="${YOURKIT_AGENT_PATH:-/opt/yourkit-agent/linux-x86-64/libyjpagent.so}"
YOURKIT_AGENT_PORT="${YOURKIT_AGENT_PORT:-10001}"
YOURKIT_FLAG=$(echo "${YOURKIT_AGENT_ENABLE:-false}" | awk '{print tolower($0)}')

if [ "$YOURKIT_FLAG" == true ] ; then
  if [ -e "${YOURKIT_AGENT_PATH}" ] ; then
    yourkit_options="-agentpath:${YOURKIT_AGENT_PATH}=port=${YOURKIT_AGENT_PORT}"
    echo "Enabling YourKit Profiling Agent"
  else
    echo "WARNING: Cannot enable YourKit Profiling Agent!"
    echo "         YourKit Agent lib does not exist at: ${YOURKIT_AGENT_PATH}"
  fi
fi

echo "Launching transactor with java opts: $JAVA_OPTS $yourkit_options"
exec java -server ${JAVA_OPTS} ${yourkit_options} \
          -XX:OnError='echo "Fatal JVM Error (pid: %p)"' \
          -XX:OnOutOfMemoryError='echo "Fatal JVM OutOfMemoryError (pid: %p)"' \
          -jar /eva-server/transactor.jar /eva-server/server-config.edn
