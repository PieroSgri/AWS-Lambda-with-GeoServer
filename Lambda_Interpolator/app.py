from chalice import Chalice, Response
from osgeo import gdal
import json
import boto3
import time
import requests

app = Chalice(app_name='Lambda_Interpolator')

tmp_filepath = '/tmp/bugs.geojson'
timestr = time.strftime("%Y%m%d-%H%M%S")
tmp_object_name = 'img/int_image_'


# object_name argument is the S3 object key in the form '<bucket_folder>/<file_name.jpg>'
def put(file_to_upload, bucket_name='awsapp-storage'):

    object_name = tmp_object_name + timestr + '.tiff'
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_to_upload, bucket_name, object_name)
        status = True
        return status, object_name
    except:
        status = False
        return status, False


def gdal_grid(path):

    grid_opt = gdal.GridOptions(format='GTiff',
                                outputType=gdal.GDT_Int16,
                                algorithm='invdist:power=3.0:smothing=0.0:radius1=0.0:radius2=0.0:angle=0.0:max_points=0:min_points=0:nodata=0.0',
                                layers='bugs',
                                zfield='value',
                                )

    # Use .tiff and not .tif
    try:
        output = gdal.Grid('/tmp/output.tiff', path, options=grid_opt)
        status = True
        return status
    except:
        status = False
        return status


@app.route('/test')
def test_version():
    string = gdal.__version__
    string = 'GDAL version' + string
    return string


@app.route('/interpolate/{geojson}', methods=['POST'])
def interpolate(geojson):

    if app.current_request.method != 'POST':
        return Response(body='Method not allowed, use POST!',
                        status_code=405,
                        headers={'Content-Type': 'text/plain'})

    headers = app.current_request.headers
    content_type = headers['content-type']

    if content_type != 'application/json':
        return Response(body='Bad request, send .geojson file content as a json body!',
                        status_code=400,
                        headers={'Content-Type': 'text/plain'})

    request = app.current_request
    geojson = request.json_body
    geojson = json.dumps(geojson)

    f = open('/tmp/bugs.geojson', 'w+')
    f.write(geojson)
    f.close()

    int_status = gdal_grid(tmp_filepath)

    if not int_status:
        return 'Interpolation failed!'

    put_status, s3_path = put(file_to_upload='/tmp/output.tiff')

    if not put_status:
        return 'Upload failed!'

    msg = 'Interpolated image uploaded at: ' + s3_path + ' soon available on Geoserver!'

    payload_dict = {'s3_path': s3_path, 'timestr': timestr}
    payload = json.dumps(payload_dict)

    headers = {
        'Content-Type': "application/json",
        'cache-control': "no-cache"
    }

    url = 'https://mgj4tiswx7.execute-api.eu-central-1.amazonaws.com/api/geopipeline/parameters'

    requests.request("POST", url, data=payload, headers=headers)

    return msg
