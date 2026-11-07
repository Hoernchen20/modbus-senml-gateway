#!/bin/sh
# Runs the gateway and restarts it if the process ever exits.
# Installed as /usr/local/sbin/run-gateway and started by /etc/start.sh.
#
# Panics inside a connection task are already handled in-process; this
# covers anything that takes the whole process down.

BIN=/usr/local/bin/modbus-senml-gateway
# The router mounts the site files from outside the container here.
CONFIG_DIR=/dev/container_config/files
CONFIG=$CONFIG_DIR/config.toml
# Holds the MQTT password, e.g. MQTT_PASSWORD=... (the variable name must
# match mqtt.password_env in the config).
ENV_FILE=$CONFIG_DIR/gateway.env
RESPAWN_DELAY=5
# stderr of the gateway: errors before logging is up, panics, and messages
# that /dev/log can't take. The router's rootfs is read-only, so this has to
# be on the /tmp tmpfs. That is RAM, so each start rotates the file.
ERROR_LOG=/tmp/modbus-gateway.err

log() {
	logger -t run-gateway -p daemon.err "$1" 2>/dev/null || echo "$1" >&2
}

child=
# Forward SIGTERM/SIGINT so the gateway can shut down gracefully.
trap 'stopping=1; [ -n "$child" ] && kill -TERM "$child" 2>/dev/null' TERM INT

while [ -z "$stopping" ]; do
	if [ ! -r "$CONFIG" ]; then
		log "config $CONFIG not readable, retrying in ${RESPAWN_DELAY}s"
	else
		# Loaded before every start: the mounted files may appear or change
		# while the container is running.
		if [ -r "$ENV_FILE" ]; then
			set -a
			. "$ENV_FILE"
			set +a
		fi
		mv -f "$ERROR_LOG" "$ERROR_LOG.old" 2>/dev/null
		# The shell doesn't run a command whose redirect fails, so a log
		# file that can't be opened must not reach the line below.
		err=$ERROR_LOG
		( : >"$err" ) 2>/dev/null || err=/dev/null
		"$BIN" "$CONFIG" 2>>"$err" &
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
