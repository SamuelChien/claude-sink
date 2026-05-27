FROM grafana/grafana:10.4.0

COPY datasources.yaml /etc/grafana/provisioning/datasources/
COPY dashboards.yaml /etc/grafana/provisioning/dashboards/
COPY dashboards/ /var/lib/grafana/dashboards/

ENV GF_SECURITY_ADMIN_PASSWORD=claudesink
ENV GF_AUTH_ANONYMOUS_ENABLED=true
ENV GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer
ENV GF_SERVER_HTTP_PORT=8080
