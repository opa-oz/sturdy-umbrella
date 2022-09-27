FROM cr.yandex/crpnpsi88vqr2fvp2ngb/base/base_golang_image:macos-m1

WORKDIR /build

COPY go.mod ./
RUN go mod download && go mod verify

COPY main.go main.go

RUN go build -v -o /usr/local/bin/app ./...

WORKDIR /opt

COPY run.sh /tmp/run.sh
RUN chmod +x /tmp/run.sh

ENTRYPOINT ["/tmp/run.sh"]
