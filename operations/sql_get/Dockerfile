FROM cr.yandex/crpnpsi88vqr2fvp2ngb/base/base_python_image:macos-m1

COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /tmp/requirements.txt

COPY run.sh /tmp/run.sh
RUN chmod +x /tmp/run.sh

COPY main.py /tmp/main.py

ENTRYPOINT ["/tmp/run.sh"]
