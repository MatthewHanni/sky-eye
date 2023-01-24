FROM public.ecr.aws/lambda/python:3.8

COPY /src .

ENV SECRET_NAME prod/hanni
ENV REGION_NAME us-east-1
ENV KEY_BUCKET_NAME skyeye-aws-adsb-bucket
ENV KEY_HOME_LATITUDE skyeye-home-latitude
ENV KEY_HOME_LONGITUDE skyeye-home-longitude
ENV KEY_ADBS_EXCHANGE_API_KEY skyeye-adbs-exchange-rapidapi-key

RUN pip3 install -r requirements.txt

CMD [ "app.handler" ]