FROM google/cloud-sdk:latest
RUN apt-get update 
RUN pip3 install --upgrade pip
RUN pip3 install pandas
RUN pip3 install google-cloud-storage
RUN pip3 install google-api-python-client

COPY ./ /root/

RUN chmod 777 -R /root
ENTRYPOINT [ "python3", "/root/read-data-gcs.py" ]