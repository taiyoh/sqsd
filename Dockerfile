FROM golang:1.13.8-buster AS sqsd-builder

RUN apt-get update && apt-get install unzip

ENV envlate_version=0.1.2
ENV envlate_zip_file=envlate-${envlate_version}.zip
RUN curl -L https://github.com/tkuchiki/envlate/releases/download/v${envlate_version}/envlate_linux_amd64.zip -o ${envlate_zip_file} && \
    unzip ${envlate_zip_file} && \
    chmod +x envlate && \
    mv envlate /usr/local/bin && \
    rm ${envlate_zip_file}

ADD . /app
WORKDIR /app

RUN make docker

FROM busybox

COPY --from=sqsd-builder /usr/local/bin/envlate /usr/local/bin/envlate
COPY --from=sqsd-builder /app/pkg/sqsd /usr/local/bin/sqsd
COPY --from=sqsd-builder /app/docker/config.toml config.toml.tpl
COPY --from=sqsd-builder /app/docker/run.sh run.sh

CMD sh run.sh
