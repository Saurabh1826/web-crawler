FROM ubuntu:22.04

RUN apt-get update && apt-get install -y python3 python3-pip

COPY requirements.txt /

RUN pip install --no-cache-dir -r requirements.txt

COPY fetch.py /

CMD ["python3", "-u", "fetch.py"]
