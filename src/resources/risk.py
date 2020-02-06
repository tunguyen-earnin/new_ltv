from flask_restplus import Namespace, Resource, fields, reqparse
from utils import ResourceException, output_json
from flask import g as request_context, request, jsonify
import ah_datadog
import datadog
import logging
import time

from modeling.misc.experiment import UserExperiment
from modeling.misc.predictor import Predictor
from modeling.model.identityrisk import *
from modeling.model.activation.ActivationRiskModel import ActivationRiskModel
from modeling.model.max_adjustment.MaxAdjustmentModel import MaxAdjustmentModel
from modeling.model.newuser.NewUserRiskModel import NewUserRiskModel
from modeling.model.restore.RestoreModel import RestoreModel


api = Namespace('risk')
logger = logging.getLogger("ah.risk")

simple_response_model = api.model('Response', {
        'Message': fields.String,
        'Status': fields.Integer
    })


@api.route("/activation/<user_id>")
@api.doc(params={'user_id': fields.Integer})
class Activation(Resource):
    @ah_datadog.datadog_timed(name="endpoint.timing", tags=["operation:activation"])
    @api.response(200, 'Success', simple_response_model)
    def get(self, user_id):
        datadog.statsd.increment(ah_datadog.get_datadog_prefix() + "requests", tags=["operation:activation"])
        uid = _validate_user_id(user_id)
        logger.info("activation():")

        result = {'userId': uid}

        # Pass the test uid successfully for alert tests:
        if uid in [0, '0']:
            return

        try:

            model = ActivationRiskModel()
            fg = model.getAllFeatures(uid)
            predictor = Predictor(fg.f, filepath=model.pkl_path)

            try:
                score = predictor.getScore()
            except:
                datadog.statsd.increment(ah_datadog.get_datadog_prefix() + "exception",
                                         tags=["operation:activation_prediction"])
                logger.exception('Activation Risk scoring default -1 assigned')
                score = -1

            if score >= 0:
                reasonCode = predictor.getReasonCode()
            else:
                reasonCode = predictor.getReasonCodeForNegativeOne()
                logger.debug('Activation Risk scoring default -1 assigned, reasonCode: %s', reasonCode)

            result['score'] = score
            result['reasonCode'] = reasonCode

            return jsonify(result)

        except:
            logger.exception('Request failed')
            datadog.statsd.increment(ah_datadog.get_datadog_prefix() + "exception", tags=["operation:activation"])
            raise ResourceException('request of activation risk failure, userid = %s.' % user_id)


@api.route("/identity/<user_id>")
@api.doc(params={'user_id': fields.Integer})
class Identity(Resource):
    @ah_datadog.datadog_timed(name="endpoint.timing", tags=["operation:identity"])
    @api.response(200, 'Success', simple_response_model)
    def get(self, user_id):
        datadog.statsd.increment(ah_datadog.get_datadog_prefix() + "requests", tags=["operation:identity"])
        uid = _validate_user_id(user_id)
        logger.info("identity():")

        # Pass the test uid successfully for alert tests:
        if uid in [0, '0']:
            return

        try:
            r = checkIdentity(uid)

            json_string = jsonify(r.toDict())
        except Exception as e:
            logger.exception('request of identity risk failed')
            datadog.statsd.increment(ah_datadog.get_datadog_prefix() + "exception", tags=["operation:identity"])
            raise ResourceException('request of identity risk failure, userid = %s.' % user_id)

        return json_string




@api.route("/max_adjustment/<user_id>")
@api.doc(params={'user_id': fields.Integer})
class MaxAdjustment(Resource):
    @ah_datadog.datadog_timed(name="endpoint.timing", tags=["operation:max_adjustment"])
    @api.response(200, 'Success', simple_response_model)
    def get(self, user_id):
        datadog.statsd.increment(ah_datadog.get_datadog_prefix() + "requests", tags=["operation:max_adjustment"])
        uid = _validate_user_id(user_id)
        logger.info("max_adjustment():")

        result = {'userId': uid}

        # Pass the test uid successfully for alert tests:
        if uid in [0, '0']:
            return

        try:
            t0 = time.time()
            model = MaxAdjustmentModel()
            fg = model.getAllFeatures(uid)

            # Model with new features
            eid = 67
            modelExp = UserExperiment()
            modelExpid = modelExp.getUserGroup(uid, eid)

            t1 = time.time()
            # lightgbm model
            if modelExpid == 2:
                predictor = lightgbm_model(fg, model)
            else:
                predictor = logistic_regression_model(fg, model)

            try:
                score = predictor.getScore()
                if modelExpid ==2:
                    logger.info('lightgbm prediction response time is %s', (time.time() - t1))
                    logger.info('lightgbm total response time is %s', (time.time() - t0))
                else:
                    logger.info('logistic regression prediction response time is %s', (time.time() - t1))
                    logger.info('logistic regression total response time is %s', (time.time() - t0))
            except:
                logger.exception("default score -1 assigned")
                model_name = "lightgbm" if modelExpid == 2 else "logisticRegression"
                datadog.statsd.increment(ah_datadog.get_datadog_prefix() + "exception",
                                         tags=["operation:max_adjustment_prediction",
                                         "model:%s" % model_name])
                score = -1

            if modelExpid == 2:
                reasonCode = []
                reasonCat = []
                logger.info('risk score = %s, reasonCode = lightgbm', score)
            else:
                if score >= 0:
                    reasonCode = predictor.getReasonCode()
                    reasonCat = predictor.getReasonCategory()
                    logger.info('risk score = %s, reasonCode = %s', score, reasonCode)
                else:
                    reasonCode = predictor.getReasonCodeForNegativeOne()
                    reasonCat = []
                    logger.error('risk score = %s, reasonCode = %s', score, reasonCode)

            weightedTipRate = predictor.getTipRate()
            totalAmount = predictor.getTotalAmount()
            avgPayrollAmount = predictor.getAvgPayroll()

            # try:
            #     predictor.__writeDB__('max_adjustment', connection, fg.f)
            # except:
            #     log.error("Max adjustmentRisk writing db (%s)(%s) "
            #               "(userid = %s):%s" %
            #               (request. remote_addr, contextid,
            #                userid, traceback.format_exc()))

            result['score'] = score
            result['reasonCode'] = reasonCode
            result['weightedTipRate'] = weightedTipRate
            result['totalAmount'] = totalAmount
            result['reasonCategory'] = reasonCat
            result['avgPayrollAmount'] = avgPayrollAmount

            logger.debug('Returned json: %s', result)
            return jsonify(result)

        except:
            logger.exception('Max adjustment risk score request failed')
            datadog.statsd.increment(ah_datadog.get_datadog_prefix() + "exception", tags=["operation:max_adjustment"])
            raise ResourceException('request of max adjustment risk failure, userid = %s.' % user_id)


@api.route("/new_user/<user_id>")
@api.doc(params={'user_id': fields.Integer})
class NewUser(Resource):
    @ah_datadog.datadog_timed(name="endpoint.timing", tags=["operation:new_user"])
    @api.response(200, 'Success', simple_response_model)
    def get(self, user_id):
        datadog.statsd.increment(ah_datadog.get_datadog_prefix() + "requests", tags=["operation:new_user"])
        uid = _validate_user_id(user_id)
        logger.info("new_user():")

        result = {'userId': uid}

        # Pass the test uid successfully for alert tests:
        if uid in [0, '0']:
            return

        try:
            model = NewUserRiskModel()
            fg = model.getAllFeatures(uid)
            predictor = Predictor(fg.f, filepath=model.pkl_path)

            try:
                score = predictor.getScore()
            except:
                logger.warning('Default score 10 assigned')
                datadog.statsd.increment(ah_datadog.get_datadog_prefix() + "exception",
                                         tags=["operation:new_user_prediction"])
                score = 10

            if score <= 1:
                reasonCode = predictor.getReasonCode()
                logger.info('risk score = %s, reasonCode = %s',
                         score, reasonCode)
            else:
                reasonCode = predictor.getReasonCodeForNegativeOne()
                logger.error('risk score = %s, reasonCode = %s',
                          score, reasonCode)

            result['score'] = score
            result['reasonCode'] = reasonCode

            return jsonify(result)

        except Exception:
            logger.exception('Request failed')
            datadog.statsd.increment(ah_datadog.get_datadog_prefix() + "exception", tags=["operation:new_user"])
            raise ResourceException('request of new user risk failure, userid = %s.' % user_id)
        finally:
            logger.info('end of request')


@api.route("/restore/<user_id>")
@api.doc(params={'user_id': fields.Integer})
class Restore(Resource):
    @ah_datadog.datadog_timed(name="endpoint.timing", tags=["operation:restore"])
    @api.response(200, 'Success', simple_response_model)
    def get(self, user_id):
        datadog.statsd.increment(ah_datadog.get_datadog_prefix() + "requests", tags=["operation:restore"])
        uid = _validate_user_id(user_id)
        logger.info("restore():")

        result = {'userId': uid}

        # Pass the test uid successfully for alert tests:
        if uid in [0, '0']:
            return

        try:
            model = RestoreModel()
            fg = model.getAllFeatures(uid)
            predictor = Predictor(fg.f, filepath=model.pkl_path)

            try:
                score = predictor.getScore()
            except:
                logger.exception('Default score -1 assigned')
                datadog.statsd.increment(ah_datadog.get_datadog_prefix() + "exception",
                                         tags=["operation:restore_prediction"])
                score = -1

            if score >= 0:
                reasonCode = predictor.getReasonCode()
                logger.info('risk score = %s, reasonCode = %s',
                                 score, reasonCode)
            else:
                reasonCode = predictor.getReasonCodeForNegativeOne()
                logger.error('risk score = %s, reasonCode = %s',
                                  score, reasonCode)

            weightedTipRate = predictor.getTipRate()
            totalAmount = predictor.getTotalAmount()
            avgPayrollAmount = predictor.getAvgPayroll()

            result['score'] = score
            result['reasonCode'] = reasonCode
            result['weightedTipRate'] = weightedTipRate
            result['totalAmount'] = totalAmount
            result['avgPayrollAmount'] = avgPayrollAmount

            return jsonify(result)

        except:
            logger.exception('Request failed')
            datadog.statsd.increment(ah_datadog.get_datadog_prefix() + "exception", tags=["operation:restore"])
            raise ResourceException('request of restore risk failure, userid = %s.' % user_id)


@ah_datadog.datadog_timed(name="model.timing", tags=["operation:lightgbm"])
def lightgbm_model(fg, model):
    filename_lgb = 'lightgbm.pkl'
    predictor = Predictor(fg.f, filename=filename_lgb, filepath=model.pkl_path)
    return predictor


@ah_datadog.datadog_timed(name="model.timing", tags=["operation:logisticRegression"])
def logistic_regression_model(fg, model):
    filename_old = 'old_logisticRegression.pkl'
    predictor = Predictor(fg.f, filename=filename_old, filepath=model.pkl_path)
    return predictor

# helper function for validating IDs
def _validate_user_id(user_id):
    try:
        request_context.extras['UserId'] = user_id
        return int(user_id)
    except ValueError:
        raise ResourceException('invalid userid %s (integer expected)' % user_id, status_code=400)


