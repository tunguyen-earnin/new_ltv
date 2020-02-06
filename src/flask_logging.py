import logging
from flask import request, g as request_context
import ah_config
import json

old_factory = logging.getLogRecordFactory()


def record_factory(*args, **kwargs):
    record = old_factory(*args, **kwargs)
    kwargs["message"] = record.msg

    kwargs["serviceName"] = ah_config.get('app.riskName', 'risk')

    if request:
        if request_context and hasattr(request_context, 'extras') and request_context.extras is not None:
            kwargs = {**kwargs, **request_context.extras}
        if hasattr(request, 'view_args') and request.view_args is not None:
            kwargs = {**kwargs, **request.view_args}
        if hasattr(request, 'remote_addr'):
            kwargs["ip"] = request.remote_addr
        if hasattr(request, 'url'):
            kwargs["urlPath"] = request.path

    record.msg = json.dumps(kwargs, separators=(',', ':'))
    return record
