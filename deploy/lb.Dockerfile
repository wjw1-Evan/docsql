# DocSQL dev-compose optional stream-LB demo image (nginx, azurelinux base).
# Same base-image rule as the root Dockerfile: docker.io is unreachable in
# this environment, mcr.microsoft.com is not. The LB is stateless dev
# infrastructure — no volumes, no uid-1000 mount points; the packaged nginx
# runs as root inside this container by default.
FROM mcr.microsoft.com/azurelinux/base/core:3.0
RUN tdnf install -y nginx && tdnf clean all
COPY deploy/lb/nginx.conf /etc/nginx/nginx.conf
# Container-side listen port matches the node port (7600); the dev compose
# publishes the cluster entry as 127.0.0.1:17690.
EXPOSE 7600
CMD ["nginx"]
