FROM python:3.10

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /tmp/requirements.txt 

COPY src/fetch_data_nasa.py /usr/local/bin/fetch-data
RUN chmod +x /usr/local/bin/fetch-data

ENTRYPOINT [ "/usr/local/bin/fetch-data" ]
CMD [ "--help" ]