FROM golang:alpine3.6

RUN apk --update add --virtual curl unzip

ENV envlate_version=0.1.2
ENV envlate_zip_file=envlate-${envlate_version}.zip
RUN curl -L https://github.com/tkuchiki/envlate/releases/download/v${envlate_version}/envlate_linux_amd64.zip -o ${envlate_zip_file} && \
    unzip ${envlate_zip_file} && chmod +x envlate && mv envlate /usr/local/bin && rm ${envlate_zip_file}

ENV SQSD_VERSION=0.0.2
RUN curl -L https://github.com/taiyoh/sqsd/releases/download/v${SQSD_VERSION}/sqsd_linux_amd64.zip -o sqsd_linux_amd64.zip && \
    unzip sqsd_linux_amd64.zip && \
    mv sqsd_linux_amd64/sqsd $GOPATH/bin/sqsd && \
    chmod +x $GOPATH/bin/sqsd

ADD config.toml config.toml.tpl
ADD run.sh run.sh

CMD sh run.sh
