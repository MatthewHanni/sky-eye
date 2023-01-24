import json
import requests
import boto3
import datetime
import pytz
import os
from botocore.exceptions import ClientError


def get_secrets():
    secret_name = os.getenv('SECRET_NAME')
    region_name = os.getenv('REGION_NAME')

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secrets = json.loads(get_secret_value_response['SecretString'])
    return secrets


def api_call(lat_home, lon_home, api_key):
    url = f"https://adsbexchange-com1.p.rapidapi.com/v2/lat/{lat_home}/lon/{lon_home}/dist/250/"

    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "adsbexchange-com1.p.rapidapi.com"
    }
    response = requests.request("GET", url, headers=headers)
    return response


def upload_json_to_s3(response, bucket_name, lat_home, lon_home):
    now_timestamp = datetime.datetime.now(pytz.timezone('US/Eastern')).strftime("%Y%m%d-%H%M%S")
    out_key = f'adsb_raw/adsb_{lat_home}_{lon_home}_250--{now_timestamp}.json'

    s3 = boto3.client('s3')
    s3.put_object(
        Body=response.text,
        Bucket=bucket_name,
        Key=out_key
    )
    return out_key


def main():

    secrets = get_secrets()

    bucket_name = secrets[os.getenv('KEY_BUCKET_NAME')]
    lat_home = secrets[os.getenv('KEY_HOME_LATITUDE')]
    lon_home = secrets[os.getenv('KEY_HOME_LONGITUDE')]
    api_key = secrets[os.getenv('KEY_ADBS_EXCHANGE_API_KEY')]

    response = api_call(lat_home=lat_home, lon_home=lon_home, api_key=api_key)
    out_key = upload_json_to_s3(response=response, lat_home=lat_home, lon_home=lon_home, bucket_name=bucket_name)

    return f"{response.json()['msg']} Processed records:{response.json()['total']} ctime{response.json()['total']} s3://{bucket_name}/{out_key}"


def handler(event, context):
    result = main()

    return {
        'headers': {'Content-Type': 'application/json'},
        'statusCode': 200,
        'body': json.dumps({"message": f"skyeyeLark invoked. {result}",
                            "event": event})
    }
