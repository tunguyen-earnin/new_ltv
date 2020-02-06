# Prometheus metrics imports + functions
from flask import request
import time
import sys
from prometheus_client import Counter, Histogram, Gauge

REQUEST_COUNT = Counter(
    'http_requests_received', 
    'Provides the count of HTTP requests that have been processed by the Python pipeline',
    ['code', 'method', 'controller', 'action']
)
REQUEST_LATENCY = Histogram('http_request_duration_seconds', 
    'The duration of HTTP requests processed by a Python application',
    ['code', 'method', 'controller', 'action']
)
REQUEST_IN_PROGRESS = Gauge('http_requests_in_progress', 
    'The number of requests currently in progress in the Python pipeline'
)
REQUEST_IN_PROGRESS.set(0)
FIVEXX_ERRORS = Counter('http_5xx_errors', 
    'How many HTTP errors with 5xx status code are returned, partitioned by HTTP method',
    ['HTTPMethod']
)
FOURXX_ERRORS = Counter('http_4xx_errors', 
    'How many HTTP errors with 4xx status code are returned, partitioned by HTTP method',
    ['HTTPMethod']
)


def start_timer():
    request.start_time = time.time()
    REQUEST_IN_PROGRESS.inc()

def stop_timer(response):
    resp_time = time.time() - request.start_time
    REQUEST_LATENCY.labels(
        response.status_code, request.method, 'NA', request.url_rule 
        ).observe(resp_time)

    try:
        if str(response.status_code)[0] == '5':
            FIVEXX_ERRORS.labels(request.method).inc()
        if str(response.status_code)[0] == '4':
            FOURXX_ERRORS.labels(request.method).inc()
    except IndexError:
        pass

    REQUEST_IN_PROGRESS.dec()
    return response
    
def record_request_data(response):
    REQUEST_COUNT.labels(
        response.status_code, request.method, 'NA', request.url_rule
            ).inc()

    return response

def setup_metrics(app):
    app.before_request(start_timer)
    app.after_request(record_request_data)
    app.after_request(stop_timer)
