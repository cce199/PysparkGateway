import sys
import os
import atexit
import pkgutil

import requests
from py4j.java_gateway import JavaGateway, GatewayParameters

from pyspark_gateway.server import HTTP_PORT, GATEWAY_PORT

class PysparkGateway(object):
    http_url = 'http://localhost:%d' % HTTP_PORT
    gateway = None

    def __init__(self):
        self.patch()
        self.check_version()
        self.start_gateway()

    @classmethod
    def open_tmp_tunnel(cls, port):
        r = requests.post(cls.http_url+'/tmp_tunnel', json={'port': port})
        return r.json()['port']

    def patch(self):
        path = os.path.dirname(os.path.realpath(__file__))+'/patch_files/java_gateway_patch.py'
        patch_file = open(path, 'r').read()

        pkg = pkgutil.get_loader('pyspark')

        path = pkg.filename.split('/')
        path.append('java_gateway.py')
        path = '/'.join(path)

        if os.path.exists(path+'c'):
            os.remove(path+'c')

        original_file = open(path, 'r').read()

        with open(path, 'w') as f:
            f.write(patch_file)

        def put_back(data, path):
            with open(path, 'w') as f:
                f.write(data)

        atexit.register(put_back, original_file, path)

    def check_version(self):
        from pyspark_gateway.spark_version import spark_version, valid_spark_version

        r = requests.get(self.http_url+'/spark_version')
        resp = r.json()

        server_major = resp['spark_major_version']
        server_minor = resp['spark_minor_version']
        server_patch = resp['spark_patch_version']

        client_major, client_minor, client_patch = spark_version()

        if not valid_spark_version():
            print('Pyspark Gateway requires Spark version >= 2.4')
            sys.exit(-1)
        elif server_major != client_major or server_minor != client_minor:
            print('Spark server version: %s' % (resp['spark_version']))
            print('Spark client version: %s' % (spark_version))

            sys.exit('Spark version mismatch')

    def start_gateway(self):
        print('Starting local java gateway')

        r = requests.get(self.http_url+'/gateway')
        resp = r.json()

        params = GatewayParameters(
                port=GATEWAY_PORT,
                auth_token=resp['auth_token'],
                auto_convert=True)

        self.gateway = JavaGateway(gateway_parameters=params)
