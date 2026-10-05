# HAProxy 3.2 + Keepalived в одном образе: два таких контейнера держат плавающий VIP по VRRP
FROM haproxy:3.2-alpine
USER root
RUN apk add --no-cache keepalived procps
COPY haproxy.cfg /usr/local/etc/haproxy/haproxy.cfg
COPY keepalived.conf.tpl /etc/keepalived/keepalived.conf.tpl
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
