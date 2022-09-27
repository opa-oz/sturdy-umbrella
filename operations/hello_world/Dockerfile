FROM cr.yandex/crpnpsi88vqr2fvp2ngb/base/base_python_image:macos-m1

VOLUME /opt
WORKDIR /opt

COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /tmp/requirements.txt

COPY hello.py /tmp/hello.py

ENTRYPOINT ["python", "/tmp/hello.py"]
