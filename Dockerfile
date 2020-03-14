FROM golang:1.13.8-buster AS sqsd-builder

ADD . /app
WORKDIR /app

RUN make docker

FROM busybox

COPY --from=sqsd-builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt 
COPY --from=sqsd-builder /app/pkg/sqsd /usr/local/bin/sqsd

CMD [ "/usr/local/bin/sqsd" ]
