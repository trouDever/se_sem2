global_defs {
  enable_script_security
  script_user root
}

# процесс haproxy жив? иначе узел уходит в FAULT и сразу отдаёт VIP (без ожидания снижения приоритета)
vrrp_script chk_haproxy {
  script "/usr/bin/pgrep -x haproxy"
  interval 1
  fall 1
  rise 2
}

vrrp_instance VI_1 {
  state ${STATE}
  interface eth0
  virtual_router_id 51
  priority ${PRIORITY}
  advert_int 1
  unicast_src_ip ${SELF_IP}
  unicast_peer {
    ${PEER_IP}
  }
  virtual_ipaddress {
    ${VIP}/24 dev eth0
  }
  track_script {
    chk_haproxy
  }
}
