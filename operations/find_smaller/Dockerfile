FROM cr.yandex/crpnpsi88vqr2fvp2ngb/base/base_jq_image:macos-m1

WORKDIR /tmp

RUN echo "#!/bin/bash" > /tmp/run.sh
RUN echo 'cat $1 | jq ".[] | select(.>$(cat $2))" | tr "\n" "," | sed "s/,$/]/" | sed "s/^/[/" | jq -sc ".[]" > $3' >> /tmp/run.sh

RUN chmod +x /tmp/run.sh

WORKDIR /opt

ENTRYPOINT ["/tmp/run.sh"]
