from chalice import Chalice
import requests
import json

app = Chalice(app_name='Lambda_Geoserver')


@app.route('/test')
def test():
    message = "Ok!"
    return message


def create_store(s3_path, timestr):
    # http://my-geoserver.univpm
    host = "localhost:8080"
    prefix = 'http://localhost:8080/geoserver/rest/workspaces'
    workspace_name = 'univpm'
    store_type = 'coveragestores'

    # Generate the url
    url_tuple = (prefix, workspace_name, store_type)
    url = '/'.join(url_tuple)

    # Generate the uri for a S3GeoTiff type store
    region = '?awsRegion=EU_CENTRAL_1'
    s3_uri = "s3://awsapp-storage" + s3_path + region

    store_name = "store" + timestr

    payload_dict = {
        "coverageStore": {
            "name": store_name,
            "enabled": "true",
            "type": "S3GeoTiff",  # S3GeoTiff (S3 plugin type) != GeoTIFF (normal type)
            "workspace": workspace_name,
            "url": s3_uri
        }
    }

    payload = json.dumps(payload_dict)

    headers = {
        'Content-Type': "application/json",
        'Authorization': "Basic YWRtaW46Z2Vvc2VydmVy",
        'Accept': "*/*",
        'Cache-Control': "no-cache",
        'Host': host,
        'Accept-Encoding': "gzip, deflate",
        'Connection': "keep-alive",
        'cache-control': "no-cache"
    }

    requests.request("POST", url, data=payload, headers=headers)

    return store_name


def publish_layer(s3_path, store):
    # http://my-geoserver.univpm
    host = "localhost:8080"
    prefix = 'http://localhost:8080/geoserver/rest/workspaces'
    workspace_name = 'univpm'
    store_type = 'coveragestores'
    store_name = store
    resource = 'coverages'

    # Generate the url
    url_tuple = (prefix, workspace_name, store_type, store_name, resource)
    url = '/'.join(url_tuple)

    # Strip s3_path to get layer name, valid only 'cause we're sure the lenght is always the same!
    layer_name = s3_path[5:-5]

    payload_dict = {
        "coverage": {
            "name": layer_name,
            "nativeName": layer_name
        }
    }

    payload = json.dumps(payload_dict)

    headers = {
        'Content-Type': "application/json",
        'Authorization': "Basic YWRtaW46Z2Vvc2VydmVy",
        'Accept': "*/*",
        'Cache-Control': "no-cache",
        'Host': host,
        'Accept-Encoding': "gzip, deflate",
        'Connection': "keep-alive",
        'cache-control': "no-cache"
    }

    requests.request("POST", url, data=payload, headers=headers)

    return layer_name


def set_style(layer_name):

    # http://my-geoserver.univpm
    host = "localhost:8080"
    prefix = 'http://localhost:8080/geoserver/rest/layers'
    layer_name = layer_name  # Just to keep things in order :)
    postfix = 'styles'

    # Generate the url
    url_tuple = (prefix, layer_name, postfix)
    url = '/'.join(url_tuple)

    query_params = {"default": "true"}

    payload_dict = {
                    "style": {
                        "name": "univpm_style"
                    }
    }

    payload = json.dumps(payload_dict)

    headers = {
        'Content-Type': "application/json",
        'Authorization': "Basic YWRtaW46Z2Vvc2VydmVy",
        'Accept': "*/*",
        'Cache-Control': "no-cache",
        'Host': host,
        'Accept-Encoding': "gzip, deflate",
        'Connection': "keep-alive",
        'cache-control': "no-cache"
    }

    requests.request("POST", url, params=query_params, data=payload, headers=headers)


@app.route('/geopipeline/{parameters}', methods=['POST'])
def geopipeline(parameters):

    # request = app.current_request
    # parameters = request.json_body
    parameters = json.loads(parameters)

    s3_path = parameters['s3_path']
    timestr = parameters['timestr']

    store_name = create_store(s3_path, timestr)
    layer_name = publish_layer(s3_path, store_name)
    set_style(layer_name)

    return


path_test = '/img/int_image_20191212-223058.tiff'
timestr_test = '20191212-223058'

params = {'s3_path': path_test, 'timestr': timestr_test}
params = json.dumps(params)

geopipeline(params)
