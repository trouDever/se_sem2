#!/bin/sh
set -e
sed -e "s/\${STATE}/$STATE/" -e "s/\${PRIORITY}/$PRIORITY/" -e "s/\${SELF_IP}/$SELF_IP/" \
    -e "s/\${PEER_IP}/$PEER_IP/" -e "s/\${VIP}/$VIP/" \
    /etc/keepalived/keepalived.conf.tpl > /etc/keepalived/keepalived.conf
keepalived --dont-fork --log-console --log-detail &
exec haproxy -W -db -f /usr/local/etc/haproxy/haproxy.cfg
