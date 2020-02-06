import unittest

from flask import Flask

import flask_logging


class TestLogging(unittest.TestCase):

    def test_logs_simple(self):
        result = flask_logging.record_factory(name='name', level='level', pathname='pathname', lineno='lineno',
                                              msg='msg', args='args', exc_info='exc_info')
        assert result is not None

    def test_logs_with_request_no_context(self):
        app = Flask("test")
        with app.test_request_context("/"):
            result = flask_logging.record_factory(name='name', level='level', pathname='pathname', lineno='lineno',
                                                  msg='msg', args='args', exc_info='exc_info')
            assert result is not None
