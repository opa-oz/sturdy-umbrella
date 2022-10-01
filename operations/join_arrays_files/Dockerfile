FROM cr.yandex/crpnpsi88vqr2fvp2ngb/base/base_jq_image:macos-m1

WORKDIR /tmp

RUN echo "#!/bin/bash" > /tmp/run.sh
RUN echo 'jq -sc "[.[][]]" input_* > ${@: -1}' >> /tmp/run.sh

RUN chmod +x /tmp/run.sh

WORKDIR /opt

ENTRYPOINT ["/tmp/run.sh"]
