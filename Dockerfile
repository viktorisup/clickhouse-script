FROM python:3.12
WORKDIR /usr/src/app
RUN pip install requests aioch
COPY gtw-txn-exp.py .
ENTRYPOINT ["python", "./gtw-txn-export.py"]