#!/bin/sh
# Runs the gateway and restarts it if the process ever exits.
# Installed as /usr/local/sbin/run-gateway and started by /etc/start.sh.
#
# Panics inside a connection task are already handled in-process; this
# covers anything that takes the whole process down.

BIN=/usr/local/bin/modbus-senml-gateway
CONFIG=/etc/modbus-gateway/config.toml
# Holds the MQTT password, e.g. MQTT_PASSWORD=... (the variable name must
# match mqtt.password_env in the config).
ENV_FILE=/etc/modbus-gateway/gateway.env
RESPAWN_DELAY=5
# stderr of the gateway; it only writes there while /dev/log is missing.
ERROR_LOG=/var/log/modbus-gateway.err

log() {
	logger -t run-gateway -p daemon.err "$1" 2>/dev/null || echo "$1" >&2
}

if [ -r "$ENV_FILE" ]; then
	set -a
	. "$ENV_FILE"
	set +a
fi

child=
# Forward SIGTERM/SIGINT so the gateway can shut down gracefully.
trap 'stopping=1; [ -n "$child" ] && kill -TERM "$child" 2>/dev/null' TERM INT

while [ -z "$stopping" ]; do
	if [ ! -r "$CONFIG" ]; then
		log "config $CONFIG not readable, retrying in ${RESPAWN_DELAY}s"
	else
		"$BIN" "$CONFIG" 2>>"$ERROR_LOG" &
		child=$!
		wait "$child"
		status=$?
		if [ -n "$stopping" ]; then
			# wait returned early because of the signal; wait for the
			# gateway to finish its shutdown.
			wait "$child"
			break
		fi
		child=
		log "gateway exited with status $status, restarting in ${RESPAWN_DELAY}s"
	fi
	# Sleep in the background so a signal interrupts the delay.
	sleep "$RESPAWN_DELAY" &
	child=$!
	wait "$child"
	child=
done
