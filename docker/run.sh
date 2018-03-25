#!/bin/sh

cat config.toml.tpl | envlate -o config.toml
sqsd -config=config.toml
