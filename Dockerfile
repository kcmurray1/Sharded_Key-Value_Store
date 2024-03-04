FROM python:3.11.2
WORKDIR /bingus
COPY . /bingus
RUN pip install -r requirements.txt
CMD python ./replica.py ${SOCKET_ADDRESS} ${VIEW}