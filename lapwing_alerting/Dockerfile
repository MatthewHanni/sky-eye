FROM public.ecr.aws/lambda/python:3.8

COPY /src .

ENV SECRET_NAME prod/hanni
ENV REGION_NAME us-east-1
ENV KEY_ALERTING_ENDPOINT_URL kahuna-url
ENV KEY_BUCKET_NAME skyeye-aws-adsb-bucket

RUN pip3 install -r requirements.txt

CMD [ "app.handler" ]