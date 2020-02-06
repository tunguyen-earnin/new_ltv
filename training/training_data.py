import ah_db
import csv
import os
import traceback
from modeling.feature.feature_activation import ActivationFeature
from modeling.feature.feature_bank import BankFeature_predict, BankFeature
from modeling.feature.feature_payroll import Payroll
from modeling.feature.feature_device import DeviceFeature
from modeling.feature.feature_user import UserFeature
from modeling.feature.feature_employment import EmploymentFeature
from modeling.feature.feature_cs import CSFeature
from datetime import datetime
from modeling.feature.featureGeneration import FeatureGenerator
from modeling.feature.feature_timesheet import TimeSheetFeature
from modeling.feature.feature_new import NewFeature
from multiprocessing import Pool


import pickle
import pandas as pd
import modeling.model.data_preparation as dp
import numpy as np


class NewUserLTVModel:
    def __init__(self):
        self.dummyVar = []

    def createDerivedFeature(self, fg):

        self.dummyVar = ['derived_employerid', 'cs_lastAudit1Username',
                         'cs_lastAuditSource', 'cs_lastAuditType',
                         'cs_lastAuditUsername', 'device_lastInstallOS',
                         'payroll_lastPayrollSetupBy',
                         'payroll_paycycleFrequencyId', 'payroll_paytypeid']

        fg.f['derived_employerid'] = fg.f['employment_employer']

        if fg.f['payroll_hourlyRate'] > 20:
            fg.f['derived_hourlyRateGT20'] = 1
        else:
            fg.f['derived_hourlyRateGT20'] = 0

        if fg.f['payroll_lastPayrollStatus'] != 11:
            fg.f['derived_lastPayrollStatus11'] = 0
        else:
            fg.f['derived_lastPayrollStatus11'] = 1

        if fg.f['cs_nCredit'] > 0:
            fg.f['derived_hasCSCredit'] = 1
        else:
            fg.f['derived_hasCSCredit'] = 0

        if fg.f['employment_nEmployer'] > 5:
            fg.f['derived_nEmployerGT5'] = 1
        else:
            fg.f['derived_nEmployerGT5'] = 0

    def getPredTimeTarget(self):
        with ah_db.open_db_engine('postgresql') as connection:
            sql = '''
              """select
                   ins.userid,
                   min_activation_date as predtime
                   ins.os,
                   ins.osversion,
                   (CASE WHEN ins.network is NULL then 'LAT' else ins.network END) as network,
                   ltv.tip_6m,
                   ltv.risk_loss_6m,
                   ltv.tip_3m,
                   ltv.risk_loss_3m,
                   ltv.tip_1m,
                   ltv.risk_loss_1m,
                   ins.time as install_date
            from analysis.installs ins
            join (select userid, requestdate as first_activation_date from analysis.activations
                        where rank =1
                        group by userid )
            on ins.userid = a.userid
            JOIN
                 (
                     select
                        userid,
                        tip_6m,
                        risk_loss_6m
                     from
                        analysis.user_ltv_detail
                     where
                        tip_6m is not null
                        and risk_loss_6m is not null ) ltv
                 ON
                     ins.userid = ltv.userid
            WHERE ins.time > date('2018-10-01') """

                '''

            return connection.execute(sql).fetchall() # return the fetch results




    def getMiscFeature(self, f, targ



    def getAllFeatures(self, uid):
     #   print("Start calculating features for %d"%uid)
        activation = ActivationFeature(uid)
        bank = BankFeature_predict(uid)
        payroll = Payroll(uid)
        device = DeviceFeature(uid)
        user = UserFeature(uid)
        employment = EmploymentFeature(uid)
        cs = CSFeature(uid)
        # pip = PIPFeature(self.conn, uid)
        timesheet = TimeSheetFeature(uid)

        new = NewFeature(uid)

        predTime = datetime.utcnow() #+timedelta(hours=-4) datetime.now(eastern)
        fg = FeatureGenerator(uid, predTime)
        self.getMiscFeaturePredict(fg.f, uid)

        fg.feature_generator(activation)
        fg.feature_generator(payroll)
        fg.feature_generator(device)
        fg.feature_generator(user)
        fg.feature_generator(employment)
        fg.feature_generator(cs)
        fg.feature_generator(bank)
        # fg.feature_generator(pip)
        fg.feature_generator(timesheet)
        fg.feature_generator(new)

        self.createDerivedFeature(fg)
        return fg
