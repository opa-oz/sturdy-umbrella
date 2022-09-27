FROM ubuntu:20.04

VOLUME /opt
WORKDIR /opt

ARG DEBIAN_FRONTEND=noninteractive

RUN rm -f /etc/dpkg/dpkg.cfg.d/excludes \
    && rm -f /etc/dpkg/dpkg.cfg.d/docker \
    && apt-get update && apt-get install -f less curl man-db manpages manpages-* nano htop mc locales -y \
    && sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
    && locale-gen

