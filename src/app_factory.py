import logging
import uuid

import datadog
from flask import Flask, request, g as request_context
from flask_logging import record_factory


def app_factory(_ah_config):
    _ah_config.initialize()
    datadog.initialize()
    _app_name = _ah_config.get('app.riskName', 'risk')

    #   Use a custom LogRecord factory to inject some request and context attributes
    logging.setLogRecordFactory(record_factory)

    _app = Flask(_app_name)

    @_app.before_request
    def before_request():
        #   Grab the ContextId if there is one or generate one
        context_id = request.headers.get('ContextId') if 'ContextId' in request.headers else str(uuid.uuid4())

        # Create the extras and store them in the request context
        request_context.extras = {
            'ContextId': context_id
        }

    return _app