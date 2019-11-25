# Create a user and group for the daemon
getent group  carbon-relay-ng >/dev/null 2>&1 \
    || groupadd -r carbon-relay-ng >/dev/null 2>&1 \
    || true
getent passwd carbon-relay-ng >/dev/null 2>&1 \
    || useradd  -g carbon-relay-ng -c "carbon-relay-ng" \
        -d /var/lib/carbon-relay-ng -M -s /sbin/nologin -r \
        carbon-relay-ng >/dev/null 2>&1 \
    || true

