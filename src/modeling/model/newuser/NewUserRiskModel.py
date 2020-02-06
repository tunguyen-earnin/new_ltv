import ah_datadog
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
from modeling.feature.feature_generator import FeatureGenerator
from modeling.feature.feature_timesheet import TimeSheetFeature
from modeling.feature.feature_new import NewFeature


import pickle
import pandas as pd
import modeling.misc.data_preparation as dp
import numpy as np


class NewUserRiskModel:
    def __init__(self):
        self.dummyVar = []
        self.pkl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "newuser_lr.pkl")

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
        with ah_db.open_db_connection('sqlserver') as connection:
            sql = '''
              select u.userid,  max(predtime) as predtime,sum(IsRestoreFailureAch) as isFail90d, sum(IsLossUpperBound) as isLoss90d,sum(amount) as amount,sum(tipamount) as tipamount,sum(IsLossUpperBound*amount) as totallossamt, max(isFail) as isFail, max(u.isLoss) as isLoss, max(userstatuschangereasonid) as userstatuschangereasonid, max(updatedby) as updatedby, max(originalstatusid) as originalstatusid, max(contributionmargin) as contributionmargin, max(amt_tot) as amt_tot, max(loss_tot) as loss_tot
                from
                    (select u.userid,case when MinActivationdate< cast(a.createdon as date) then cast(MinActivationdate as datetime) else a.createdon end as predtime, case when restorefailureamount>0 then 1 else 0 end as isFail, case when restorefailureamount>0 and restorefailureamount>recoveredamount+1 then 1 else 0 end as isLoss,contributionmargin,a.userstatuschangereasonid, a.updatedby,a.originalstatusid,newstatusid,u.amount as amt_tot, u.restorefailureamount-recoveredamount as loss_tot
                    from  (select userid,userstatuschangereasonid, updatedby,createdon,originalstatusid,newstatusid
                        from (select *,rank() over (Partition by userid order by createdon) ranks
                        from [Users].[UserStatusHistory]
                        where NewStatusId=4 or newstatusid=8) a
                        where ranks=1) a
                    left join  analysis.users u on u.UserID=a.userid
                    where u.signupdate>='2016-09-01'
                ) u
                left join  analysis.activations ac on u.userid=ac.userid and predtime> dateadd(dd,-90,ac.RequestDate)  and iserror=0 and IsCancelled=0 and ac.restoredate<dateadd(dd,-7,getdate())
                where predtime<getdate()-60
                group by u.userid
                --having sum(IsRestoreFailureAch) is not null
                '''

            return connection.execute(sql).fetchall()

    def getMiscFeature(self, f, target):
        f['IsLoss']=target['isLoss90d']
        f['IsFail']=target['isFail90d']
        try:
            f['Contribution']=target['tipamount']-target['totallossamt']
        except:
            f['Contribution']=0
        f['userstatuschangereasonid']=target['userstatuschangereasonid']
        f['updatedby']=target['updatedby']
        f['originalstatusid']=target['originalstatusid']

    def getMiscFeaturePredict(self, f, UserId):
        sql = '''
        select userid,userstatuschangereasonid, updatedby,createdon,originalstatusid
        from miscellaneous.UserStatusHistory
        where NewStatusId=4 AND userid = {}
        ORDER BY CreatedOn DESC
        LIMIT 1'''.format(str(UserId))

        r = ah_db.execute_to_json('miscellaneous', sql)
        if len(r) > 0:
            target = r[0]
        else:
            target = {'userstatuschangereasonid': 'NaN',
                      'updatedby': 'NaN',
                      'originalstatusid': 'NaN'}

        f['userstatuschangereasonid'] = target['userstatuschangereasonid']
        f['updatedby'] = target['updatedby']
        f['originalstatusid'] = target['originalstatusid']

    def createMaster(self, fout, targets):
        writer = None
        n = 1
        print("Start creating master data")
        print("Total number of user %d" % len(targets))
        for target in targets:
            uid=int(target['userid'])
            if n % 1000 == 0:
                print(n)
            n += 1
            #     activation = ActivationFeature(self.conn, uid,scoring=False)
            while True:
                try:
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
                    break
                except:
                    traceback.print_exc()

            if predTime > datetime(2016, 9, 1):
                fg = FeatureGenerator(uid, predTime)

                self.getMiscFeature(fg.f, target)
        #        fg.feature_generator(activation)
                fg.feature_generator(bank)
                fg.feature_generator(payroll)
                fg.feature_generator(device)
                fg.feature_generator(user)
                fg.feature_generator(employment)
                fg.feature_generator(cs)
                # fg.feature_generator(pip)
                fg.feature_generator(timesheet)
                fg.feature_generator(new)

                self.createDerivedFeature(fg)
                if writer is None:
                    fieldnames = sorted(list(fg.f.keys()))
                    writer = csv.DictWriter(fout, fieldnames=fieldnames)

                    writer.writeheader()

                fg.printFeatures(writer)

    @ah_datadog.datadog_timed(name="getAllFeatures", tags=["operation:newUserRisk"])
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

    def __loadModel__(self):
        filename = open('./modeling/model/risk_model/newuser_risk_model'
                        '/model/logisticRegression.pkl', 'rb')
        dummy_rules = pickle.load(filename)
        normalizer = pickle.load(filename)
        lr = pickle.load(filename)
        f_columns = pickle.load(filename)
        return dummy_rules, normalizer, lr, f_columns

    def getPredictionData(self, features):
        dummy_rules, normalizer, lr, f_columns = self.__loadModel__()
        df = pd.DataFrame(features, index=[0])
        df.replace(['True', 'False'], [1, 0], inplace=True)

        cats = ['employment_paytypeid', 'payroll_paycycleFrequencyId',
                'bank_getInstitutionId',
                'cs_lastAuditUsername', 'cs_lastAudit1Username', 'user_domain',
                'payroll_lastPayrollSetupBy', 'device_lastInstallOS',
                'employment_employer',
                'actDayOfWeek']

        df = dp.dummyCoding(df, cats, dummy_rules)

        ##################
        df['derived_transactionAmountTrendDisToOne'] = \
            np.abs(df['bank_transactionAmountTrend'] - 1)

        df['derived_transactionCountTrendDisToOne'] = \
            np.abs(df['bank_transactionCountTrend'] - 1)
        ###############

        X_predict = dp.dataNormalization_Predict(
            df[f_columns],
            normalizer)
        return X_predict, lr

    def getReasonCode(self, X_predict, predictor):
        rc = X_predict.values*predictor.coef_
        rc = rc[0]

        f_columns = X_predict.columns.tolist()
        reasonCodes = [(x, y) for (y, x) in
                       sorted(zip(rc, f_columns), reverse=True)]
        reasonCode = ["%s:%.2f" % x for x in reasonCodes[:5]]
        return reasonCode

    def getReasonCodeForNegativeOne(self, X_predict):
        reasons = X_predict.isnull().any(axis=0)
        reasonCode = reasons[reasons == True].index.tolist()
        return reasonCode

    def getScore(self, X_predict, predictor):

        y_pred = predictor.predict_proba(X_predict)
#        y_pred = lr.predict_proba(X_predict)

        r = y_pred[0, -1]

        return r


def prepareTrainingData(uids):
    filepath = './modeling/model/newuser/data/'
    filename = 'master_%d.csv'%os.getpid()

    while True:
        try:
            fout = open(filepath+filename, 'w', encoding='utf-8')
            break
        except IOError:
            os.mkdir(filepath)

    model = NewUserRiskModel()

    #     #    print (fg.f)

    model.createMaster(fout,uids)

    fout.close()


if __name__ == "__main__":
    model = NewUserRiskModel()

    """
    filepath = './modeling/model/risk_model/newuser_risk_model/data/'
    cmd = 'rm -f ' + filepath + 'master*.csv'
    os.system(cmd)
    conn = connect_db(30)
    model = NewUserRiskModel(conn)
    targets = model.getPredTimeTarget()
    print(len(targets))
     #   print(uids[0])
#    prepareTrainingData(targets)
    from numpy import *
    nprocess=8
    targets=[ [targets[i] for i in range(len(targets)) if i%nprocess==x] for x in range(nprocess)]
    for i in targets: print(len(i))
    pool = Pool(processes=nprocess)
    pool.map(prepareTrainingData, targets)
    """
