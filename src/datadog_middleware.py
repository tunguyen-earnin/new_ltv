from flask import request
from ah_datadog import get_datadog_prefix as dd_prefix
from datadog import statsd


def dd_response_recorder(response):
    method = request.method
    url = request.url_rule

    if url:
        tags = ["operation:" + method + "_" + str(url), "statusCode:" + str(response.status_code)]
    else:
        tags = ["statusCode:" + str(response.status_code)]
    statsd.increment(dd_prefix() + "responses", tags=tags)
    return response


def dd_error_response_catch(exception):
    if exception is not None:
        method = request.method
        url = request.url_rule

        if url:
            tags = ["operation:" + method + "_" + str(url), "statusCode:500"]
        else:
            tags = ["statusCode:500"]
        statsd.increment(dd_prefix() + "responses", tags=tags)


def setup_metrics(app):
    app.after_request(dd_response_recorder)
    app.teardown_request(dd_error_response_catch)
