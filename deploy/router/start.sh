#!/bin/sh
# Invoked by the router when the container boots. Starts syslog (unless
# the router already provides /dev/log) and the gateway, then returns.
#
# This script must terminate: the router docs say the container is not
# reachable otherwise. Everything long-running has to be started in
# the background.

# The router's serial port. Its device node isn't created automatically;
# the router docs specify creating it here. Use serial_port = "/dev/ttyS1"
# for RTU connections.
if [ ! -e /dev/ttyS1 ]; then
	mknod /dev/ttyS1 c 207 17
fi

# The gateway logs directly to /dev/log.
if [ ! -S /dev/log ]; then
	syslogd
fi

# setsid: detach from this script so the gateway keeps running after
# start.sh returns.
setsid /usr/local/sbin/run-gateway </dev/null >/dev/null 2>&1 &
