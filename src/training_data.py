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

    def getPredTimeTarget(self):

        sql =
              """SELECT
                  u2.userid,
                  u2.cashoutamount/100 as first_cashout_amt,
                  u2.tip as first_cashout_tip,
                  u2.rateperhour as average_rate_per_hour,
                  u1.first_co_date as predTime
              FROM(
              (SELECT
                  userid,
                  cashoutamount,
                  tip,
                  rateperhour,
                  createdon
              FROM
                  moneymovement.activations
              WHERE
                  hourstatusid = 1 or hourstatusid = 2) u2
              JOIN
              (SELECT
                  userid,
                  MIN(createdon) as first_co_date
              FROM
                  moneymovement.activations
              GROUP BY 1
              ) u1
              ON
                  u1.userid=u2.userid and
                  u1.first_co_date=u2.createdon) """

        result = ah_db.execute_to_json('moneyMovement', sql)
 # return the fetch results


    def createMaster(self, fout, targets):
        writer = None
        n = 1
        print("Start creating master data")
        print("Total number of user %d" % len(targets))
        while len(targets) > 0:
            if n % 1000 == 0:
                print(n)
            n += 1
            print('left!!!!!!!!----------len_uids-----'+str(len(targets))+'-------'+str(str(fout)[79:]))
            #     activation = ActivationFeature(self.conn, uid,scoring=False)
            try:
                target = targets.pop()
                uid = int(target['userid'])
                bank = BankFeature(uid)
                payroll = Payroll(uid)
                device = DeviceFeature(uid)
                user = UserFeature(uid)
                employment = EmploymentFeature(uid)
                cs = CSFeature(uid)
                # pip = PIPFeature(self.conn, uid)
                timesheet = TimeSheetFeature(uid)
                new = NewFeature(uid)

                predTime = target['predtime']
                #break
            except:
                print(uid, traceback.format_exc())
                targets.append(target)
                return targets

            if predTime > datetime(2018, 11, 1):
                fg = FeatureGenerator(uid, predTime)

                self.getMiscFeature(fg.f, target)
                    #   fg.feature_generator(activation)
                fg.feature_generator(bank)
                fg.feature_generator(payroll)
                fg.feature_generator(device)
                fg.feature_generator(user)
                fg.feature_generator(employment)
                fg.feature_generator(cs)
                # fg.feature_generator(pip)
                fg.feature_generator(timesheet)
                fg.feature_generator(new)
                try:
                    self.createDerivedFeature(fg)
                except:
                    print(uid)
                    print(traceback.format_exc())
                if writer is None:
                    fieldnames = list(fg.f.keys())
                    writer = csv.DictWriter(fout, fieldnames=fieldnames)
                    writer.writeheader()

                fg.printFeatures(writer)

        return  writer

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





def prepareTrainingData(uids):
    filepath = './modeling/model/risk_model/newuser_risk_model/data/'
    filename = 'master_%d.csv'%os.getpid()

    while True:
        try:
            fout = open(filepath+filename, 'w', encoding='utf-8')
            break
        except IOError:
            os.mkdir(filepath)

    model = NewUserLTVModel()
    while len(uids) > 0:
        uids = model.createMaster(fout, uids)

    #     #    print (fg.f)

    fout.close()





if __name__ == "__main__":
    import ah_config
    ah_config.initialize()
    filepath = './modeling/model/risk_model/newuser_risk_model/data/'

    cmd = 'rm -f ' + filepath + 'master*.csv'
    os.system(cmd)
    model = NewUserLTVModel()
    from numpy import *

    targets = model.getPredTimeTarget()
    print(len(targets))
    nprocess=6
    targets=[ [targets[i] for i in range(len(targets)) if i%nprocess==x] for x in range(nprocess)]
   #print(targets)
    for i in targets:
        print(len(i))

    pool = Pool(processes=nprocess)
    while sum(list(map(lambda x: len(x), targets))) > 0:
        try:
            targets = pool.map(prepareTrainingData, targets)
        except:
            print(datetime.now(), "restarted data generation")
