# Контейнер для запуска Ansible с Windows-хоста: ansible-core + kubernetes.core + helm (без apt)
FROM python:3.12-slim
RUN python -c "import urllib.request,tarfile,io; d=urllib.request.urlopen('https://get.helm.sh/helm-v3.19.0-linux-amd64.tar.gz').read(); t=tarfile.open(fileobj=io.BytesIO(d)); f=t.extractfile('linux-amd64/helm'); open('/usr/local/bin/helm','wb').write(f.read())" \
 && chmod +x /usr/local/bin/helm \
 && pip install --no-cache-dir ansible-core==2.19.* kubernetes==33.* \
 && ansible-galaxy collection install kubernetes.core
WORKDIR /work
