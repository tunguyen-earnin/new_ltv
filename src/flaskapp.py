import ah_config
from utils import ResourceException

from flask import jsonify, Response
from flask_restplus import Api, Resource
from resources.control import api as control_ns
from resources.risk import api as risk_ns

import prometheus_client
from app_factory import app_factory
from middleware import setup_metrics
from datadog_middleware import setup_metrics as setup_dd_metrics

#   Use a custom LogRecord factory to inject some request and context attributes
app = app_factory(ah_config)
api = Api(app, version='1.0', doc='/swagger')
setup_metrics(app)
setup_dd_metrics(app)

#
#   Handle our ResourceException
#
@api.errorhandler(ResourceException)
def handle_resource_exception(resource_exception):
    app.logger.exception("handle_resource_exception")

    #   Turn the exception into a flask Response and add the status code
    response = jsonify(resource_exception.to_dict())
    response.status_code = resource_exception.status_code
    return response


#
#   Handle all other exceptions
#
@api.errorhandler(Exception)
def all_exception_handler(ex):
    app.logger.exception("all_exception_handler")
    response = jsonify({'message': str(ex)})
    response.status_code = 500
    return response


api.add_namespace(control_ns)
api.add_namespace(risk_ns, path='/api')

CONTENT_TYPE_LATEST = str('text/plain; version=0.0.4; charset=utf-8')


@api.route('/metrics/', strict_slashes=False, doc=False)
class Metrics(Resource):
    def get(self):
        return Response(prometheus_client.generate_latest(), mimetype=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    #app.run(host='0.0.0.0')
    app.run()
