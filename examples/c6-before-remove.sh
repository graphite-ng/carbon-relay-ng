# On removal, stop and un-register service
service carbon-relay-ng stop    >/dev/null 2>&1 || true
chkconfig --del carbon-relay-ng >/dev/null 2>&1 || true

