FROM alpine:3.7

RUN apk --update add --virtual curl unzip

ENV envlate_version=0.1.2
ENV envlate_zip_file=envlate-${envlate_version}.zip
RUN curl -L https://github.com/tkuchiki/envlate/releases/download/v${envlate_version}/envlate_linux_amd64.zip -o ${envlate_zip_file} && \
    unzip ${envlate_zip_file} && chmod +x envlate && mv envlate /usr/local/bin && rm ${envlate_zip_file}

ENV SQSD_VERSION=0.0.5
RUN curl -L https://github.com/taiyoh/sqsd/releases/download/v${SQSD_VERSION}/sqsd_${SQSD_VERSION}_linux_amd64.tar.gz -o sqsd_linux_amd64.tar.gz && \
    tar xvzf sqsd_linux_amd64.tar.gz && \
    mv sqsd $GOPATH/bin/sqsd && \
    chmod +x $GOPATH/bin/sqsd

ADD config.toml config.toml.tpl
ADD run.sh run.sh

CMD sh run.sh
