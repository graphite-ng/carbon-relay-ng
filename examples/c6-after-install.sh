install -d -o carbon-relay-ng -g carbon-relay-ng -m 750 /var/log/carbon-relay-ng || true
# On initial install, register service
chkconfig --add carbon-relay-ng >/dev/null 2>&1 || true

