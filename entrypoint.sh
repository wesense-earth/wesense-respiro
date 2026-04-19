#!/bin/sh
# entrypoint.sh — Fix directory ownership then drop to PUID:PGID
set -e

PUID="${PUID:-1000}"
PGID="${PGID:-1000}"

mkdir -p /app/data
chown -R "$PUID:$PGID" /app/data /app/public

# If Docker socket is mounted, include its group as a supplementary group
# so the non-root process can read container stats.
SETPRIV_GROUPS="--clear-groups"
if [ -S /var/run/docker.sock ]; then
    DOCKER_GID=$(stat -c '%g' /var/run/docker.sock 2>/dev/null)
    if [ -n "$DOCKER_GID" ] && [ "$DOCKER_GID" != "0" ]; then
        SETPRIV_GROUPS="--groups=$DOCKER_GID"
    fi
fi

exec setpriv --reuid="$PUID" --regid="$PGID" $SETPRIV_GROUPS \
    npm start "$@"
