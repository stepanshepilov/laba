FROM alpine:3.8
RUN set -ex && \
    apk add --no-cache gcc musl-dev
WORKDIR /app
COPY ./producer_consumer.c .
RUN gcc -o producer_consumer producer_consumer.c -pthread
CMD ["./producer_consumer"]