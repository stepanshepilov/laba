FROM alpine:latest

RUN apk add --no-cache bash util-linux

COPY script.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

CMD ["/entrypoint.sh"]