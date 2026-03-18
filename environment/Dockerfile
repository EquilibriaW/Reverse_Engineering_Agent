FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1     PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends     curl     jq     procps     netcat-openbsd     ca-certificates     util-linux     && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir     fastapi==0.115.0     uvicorn==0.30.6     requests==2.32.3     pytest==8.3.3

RUN useradd -m -u 1000 agent

RUN mkdir -p /opt/internal/reference_service /opt/internal/control /workspace/probe_lab /workspace/candidate /workspace/scratch /logs/verifier /var/lib/reference_runtime

COPY hidden/internal/runtime_contract_service.py /opt/internal/reference_service/runtime_contract_service.py
COPY hidden/internal/lab_control_plane.py /opt/internal/control/lab_control_plane.py
COPY visible/probe_lab/ /workspace/probe_lab/
COPY visible/candidate.README.txt /workspace/candidate/README.txt
COPY container-entrypoint.sh /usr/local/bin/container-entrypoint.sh

RUN chmod 700 /opt/internal /opt/internal/reference_service /opt/internal/control &&     chmod 600 /opt/internal/reference_service/runtime_contract_service.py /opt/internal/control/lab_control_plane.py &&     chmod 755 /usr/local/bin/container-entrypoint.sh &&     chmod 755 /workspace/probe_lab/*.sh &&     chmod 644 /workspace/probe_lab/README.md /workspace/candidate/README.txt &&     chown -R root:root /opt/internal &&     chown -R agent:agent /workspace /logs/verifier

WORKDIR /workspace
ENTRYPOINT ["/usr/local/bin/container-entrypoint.sh"]
CMD ["sleep", "infinity"]
