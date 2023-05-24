import flask
import time
import atexit
import signal
import os
import logging

logger = logging.getLogger()

from flask import Flask, request, jsonify
from flask.logging import default_handler

from pyspark_gateway.tunnel import TunnelProcess

HTTP_PORT = 25000
GATEWAY_PORT = 25001
TEMP_PORT = 25002

GATEWAY = None
TMP_PROC = None

app = Flask(__name__)

@app.route('/gateway')
def gateway():
    body = {'auth_token': GATEWAY.gateway_parameters.auth_token}

    return jsonify(body)

@app.route('/tmp_tunnel', methods=['POST'])
def temp_tunnel():
    global TMP_PROC, TEMP_PORT

    if TMP_PROC != None:
        TMP_PROC.proc.terminate()
        TMP_PROC.proc.join(1)

    req = request.json

    logger.info('Opening temporary tunnel from port %d to %d' % (req['port'], TEMP_PORT))

    TMP_PROC = TunnelProcess(TEMP_PORT, req['port'])
    TMP_PROC.proc.join(1)

    return jsonify({'port': TEMP_PORT})

@app.route('/spark_version', methods=['GET'])
def spark_version():
    from pyspark_gateway.spark_version import spark_version
    from pyspark_gateway.version import __version__

    major, minor = spark_version()

    resp = {
        'spark_major_version': major,
        'spark_minor_version': minor,
        'pyspark_gateway_version': __version__
        }

    return jsonify(resp)

def run(*args, **kwargs):
    global GATEWAY

    if GATEWAY == None:
        from pyspark.java_gateway import launch_gateway
        from pyspark import SparkConf 
        
        access_key = os.environ.get('AWS_ACCESS_KEY', '-')
        secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', '-') 
        
        config = {
            # "spark.kubernetes.container.image": "saemaromoon/spark:driver-3.2.1-hadoop-3.2.0-aws-v221108.1",
            "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-shared-volume.options.claimName": "spark-shared-volume",
            "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-shared-volume.mount.path": "/home/jovyan",   
            "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
            "spark.sql.catalogImplementation":"hive",  
            "spark.driver.extraClassPath": "/opt/spark/jars/*:/opt/spark/emr-lib/*",
            "spark.driver.extraLibraryPath": "/opt/spark/emr-lib/native/",
            "spark.executor.extraClassPath": "/opt/spark/emr-lib/*",
            "spark.executor.extraLibraryPath":"/opt/spark/emr-lib/native/",
            "spark.driver.blockManager.port": "7777",
            "spark.driver.port": "2222",  
            "spark.driver.bindAddress": "0.0.0.0",  
            "spark.hadoop.fs.s3a.access.key": access_key,
            "spark.hadoop.fs.s3a.secret.key": secret_key,
            "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-1.amazonaws.com",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.com.amazonaws.services.s3.enableV4": "true", 
            "spark.io.encryption.enabled": "true",
            # "spark.extraListeners": "sparkmonitor.listener.JupyterSparkMonitorListener"
        }    
        spark_config = SparkConf()
        spark_config.setMaster("k8s://https://kubernetes.default.svc.cluster.local")
 
        for key, value in config.items():
            spark_config.set(key, value) 

        GATEWAY = launch_gateway(conf = spark_config)
        TunnelProcess(GATEWAY_PORT, GATEWAY.gateway_parameters.port, keep_alive=True)

    if 'debug' not in kwargs or ('debug' in kwargs and kwargs['debug'] == False):
        app.logger.removeHandler(default_handler)
        app.logger = logger

        logger.info('Starting pyspark gateway server')

    if 'port' not in kwargs:
        kwargs['port'] = HTTP_PORT

    kwargs['host'] = '0.0.0.0'

    app.run(*args, **kwargs)

if __name__ == '__main__':
    run(debug=True, use_reloader=False, port=HTTP_PORT)
