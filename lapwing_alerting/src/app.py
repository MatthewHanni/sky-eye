import json
import requests
import boto3
import datetime
import pytz
import os
import pandas as pd
import aws_helper
from botocore.exceptions import ClientError
from operator import itemgetter
import math
import intersection
from collections import namedtuple

TMP_PATH = '/tmp/tmp.json'


Point = namedtuple('Point', 'lat lon')
sides = {'north-west': Point(lat=40.778359, lon=-75.370324),
         'north-east': Point(lat=40.781476, lon=-75.286990),
         'south-west': Point(lat=40.697236, lon=-75.293060),
         'south-east': Point(lat=40.702459, lon=-75.253274)}


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


def get_future_lat_lon(ac_record):
    lat_cur = ac_record['lat']
    lon_cur = ac_record['lon']
    gs_knots = ac_record['gs']
    nav_heading = ac_record['nav_heading']
    TIME_DELTA_3_MIN = 3.0 / 60.0
    GS_TO_KNOTS = 1.15078
    # 1. Convert the ground speed from knots to nautical miles per hour (nm/h). This can be done by multiplying the ground speed by 1.15078.
    gs_nmh = gs_knots * GS_TO_KNOTS
    # 2. Convert the directional heading from degrees to radians. This can be done by multiplying the directional heading by pi/180.
    heading_radians = nav_heading * (math.pi / 180)
    lat_cur_radians = lat_cur * (math.pi / 180)
    # 3. Calculate the change in latitude and longitude
    lat_delta = (gs_nmh * math.cos(heading_radians)) / 60 * TIME_DELTA_3_MIN
    lon_delta = (gs_nmh * math.sin(heading_radians)) / (60 * math.cos(lat_cur_radians)) * TIME_DELTA_3_MIN
    # 4. Add the change in latitude and longitude to the initial latitude and longitude to get the future position of the aircraft.
    lat_predicted = lat_cur + lat_delta
    lon_predicted = lon_cur + lon_delta
    return lat_predicted, lon_predicted


def check_required_fields(ac_record):
    required_fields = ['nav_heading', 'lat', 'lon', 'gs']
    for field in required_fields:
        if field not in ac_record:
            return False
    return True


def main(event):
    secrets = get_secrets()

    alerting_endpoint = secrets[os.getenv('KEY_ALERTING_ENDPOINT_URL')]

    s3_client = aws_helper.S3Helper()
    bucket = secrets[os.getenv('KEY_BUCKET_NAME')]

    if os.path.exists(fr'C:\Users\mhann'):
        key_list = s3_client.list_objects(bucket=bucket,prefix='adsb_raw')
        key = max(key_list, key=lambda x: x['LastModified'])['Key']

        tmp_path = 'tmp.json'
    else:
        key = event['Records'][0]['s3']['object']['key']
        tmp_path = '/tmp/tmp.json'

    print(key)
    s3_client.download_file(bucket=bucket, key=key, local_path=tmp_path)

    with open(tmp_path) as json_file:
        try:
            data = json.load(json_file)
        except Exception as e:
            return f'Unable to read file as json. {str(e)}'

        if 'ac' not in data:
            return 'Unexpected JSON format. No "ac".'

    msg_list = []
    for ac_record in data['ac']:
        valid_record = check_required_fields(ac_record=ac_record)
        if not valid_record:
            continue

        flight = ac_record['flight'] if 'flight' in ac_record else 'UNKNOWN FLIGHT'
        lat_future, lon_future = get_future_lat_lon(ac_record=ac_record)

        start_point = Point(lat=ac_record['lat'], lon=-ac_record['lon'])
        end_point = Point(lat=lat_future, lon=lon_future)

        intersect_north = intersection.doIntersect(p1=start_point, q1=end_point, p2=sides['north-west'],
                                                   q2=sides['north-east'])
        intersect_east = intersection.doIntersect(p1=start_point, q1=end_point, p2=sides['south-east'],
                                                  q2=sides['north-east'])
        intersect_south = intersection.doIntersect(p1=start_point, q1=end_point, p2=sides['south-west'],
                                                   q2=sides['south-east'])
        intersect_west = intersection.doIntersect(p1=start_point, q1=end_point, p2=sides['north-west'],
                                                  q2=sides['south-east'])

        nav_heading = ac_record['nav_heading']
        if intersect_north or intersect_south or intersect_east or intersect_west:
            gs_knots = ac_record['gs']
            nav_heading = ac_record['nav_heading']
            print(f'{flight} {nav_heading} {gs_knots}k/h {lat_future},{lon_future}')
            if intersect_north or intersect_north:

                if 90 < nav_heading < 270:
                    msg_list.append(f'{flight} approaching from the north')
                else:
                    msg_list.append(f'{flight} approaching from the south')

            elif intersect_east or intersect_west:
                if 0 < nav_heading < 180:
                    msg_list.append(f'{flight} approaching from the west')
                else:
                    msg_list.append(f'{flight} approaching from the east')
            else:
                msg_list.append(f'{flight} approaching.')

    if len(msg_list) > 0:
        all_msg = '\n'.join(msg_list)
        print(all_msg)
        #requests.post(url=alerting_endpoint, json={'message': all_msg})


def handler(event, context):
    result = main(event=event)

    return {
        'headers': {'Content-Type': 'application/json'},
        'statusCode': 200,
        'body': json.dumps({
            "event": event})
    }
if __name__ == '__main__':
    print(main(event=None))