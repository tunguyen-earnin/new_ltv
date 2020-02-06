from flask_restplus import Namespace, Resource
from flask import jsonify
from utils import ResourceException, output_json
import logging
import time

api = Namespace('control')
logger = logging.getLogger("ah.control")

@api.route("/ready", doc=False)
class Ready(Resource):
    def get(self):
        logger.info("ready()")
        return jsonify({'message': 'ready'})


@api.route("/healthy", doc=False)
class Healthy(Resource):
    def get(self):
        return jsonify({'healthy': 'true'})


@api.route("/sleep/<seconds>", doc=False)
class Sleep(Resource):
    def get(self, seconds):
        seconds = int(seconds)
        logger.info("Sleeping for %d seconds...", seconds)
        time.sleep(seconds)
        return jsonify({'slept': 'true'})


@api.route("/exception", doc=False)
class Exception(Resource):
    def get(self, message):
        # raise TypeError("crap a TypeError")
        raise ResourceException(message, status_code=410)
