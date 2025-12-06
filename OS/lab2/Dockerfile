FROM alpine:3.8
RUN set -ex && \
    apk add --no-cache gcc musl-dev
WORKDIR /app
COPY ./client.c .
COPY ./server.c .

RUN gcc -o client client.c
RUN gcc -o server server.c

EXPOSE 12345