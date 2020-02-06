import os

import ah_datadog

from modeling.feature.feature_activation import ActivationFeature, Restore
from modeling.feature.feature_bank import BankFeature_predict, BankFeature
from modeling.feature.feature_payroll import Payroll
from modeling.feature.feature_device import DeviceFeature
from modeling.feature.feature_user import UserFeature
from modeling.feature.feature_employment import EmploymentFeature
from modeling.feature.feature_cs import CSFeature
from modeling.feature.feature_timesheet import TimeSheetFeature
from modeling.feature.feature_new import NewFeature
from datetime import datetime
from modeling.feature.feature_generator import FeatureGenerator
from modeling.misc.misc import get_df
import csv

from multiprocessing import Pool


class RestoreModel:
    def __init__(self):
        self.dummyVar=[]
        self.pkl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "restore_lr.pkl")

    def getAllUserId(self):
        startdate = '2018-01-01'
        enddate = '2018-02-28'

        sql1 = '''
                SELECT  distinct ft.userid
                FROM fundstransfers ft
                JOIN transferattempts ta on ta.FundsTransferId = ft.FundsTransferId
                WHERE ft.FundsTransferReasonId=2 and ft.postingdate between '{0}' and '{1}'
                AND ta.FundsTransferStatusId in (5,7) 
                AND ta.TransferAttemptId = GetDefiningTransferAttemptId(ft.FundsTransferId)
                ORDER BY ft.userid
                '''.format(startdate, enddate)

        df1 = get_df('moneyMovement', sql1)

        sql2 = '''SELECT userid from testusers'''
        df2 = get_df('miscellaneous', sql2)

        df = df1.merge(df2, on='userid', how='left', indicator=True)
        df = df['userid'][df['_merge'] == 'left_only']
        return [l for l in df]


    def createDerivedFeature(self,fg):
        self.dummyVar=['actDayOfWeek','derived_employerid','cs_lastAudit1Username','cs_lastAuditSource','cs_lastAuditType',
                       'cs_lastAuditUsername','device_lastInstallOS','payroll_lastPayrollSetupBy','payroll_paycycleFrequencyId','payroll_paytypeid'
                       ]

        if fg.f['activation_aveAmtPerAct_Wk12']>99.9: fg.f['derived_aveAmtPerAct_Wk12_100']=1
        else: fg.f['derived_aveAmtPerAct_Wk12_100']=0

        if fg.f['activation_aveCntActCancel_Wk2']>0: fg.f['derived_aveCntActCancel_Wk2_GT0']=1
        else: fg.f['derived_aveCntActCancel_Wk2_GT0']=0

        if fg.f['activation_aveDayGapAct_Wk2']>13: fg.f['derived_aveDayGapAct_Wk2_14']=1
        else: fg.f['derived_aveDayGapAct_Wk2_14']=0

        if fg.f['activation_aveTip_Wk2']>9: fg.f['derived_aveTip_Wk2_GT9']=1
        else: fg.f['derived_aveTip_Wk2_GT9']=0

        if fg.f['activation_aveTip_Wk12']>9: fg.f['derived_aveTip_Wk12_GT9']=1
        else: fg.f['derived_aveTip_Wk12_GT9']=0

        fg.f['derived_daySinceLastFailRestoreGT90']=fg.f['derived_daySinceLastFailRestore61to90']=fg.f['derived_daySinceLastFailRestore31to60']=fg.f['derived_daySinceLastFailRestore15to30']=fg.f['activation_derived_daySinceLastFailRestoreLT15']=0
        if fg.f['activation_daySinceLastFailRestore']>90: fg.f['derived_daySinceLastFailRestoreGT90']=1
        elif fg.f['activation_daySinceLastFailRestore']>60: fg.f['derived_daySinceLastFailRestore61to90']=1
        elif fg.f['activation_daySinceLastFailRestore']>30: fg.f['derived_daySinceLastFailRestore31to60']=1
        elif fg.f['activation_daySinceLastFailRestore']>14: fg.f['derived_daySinceLastFailRestore15to30']=1
        else: fg.f['activation_derived_daySinceLastFailRestoreLT15']=1

        fg.f['derived_daySinceLastRestoreGT30']=fg.f['derived_daySinceLastRestore15to30']=fg.f['derived_daySinceLastRestoreLT15']=0
        if fg.f['activation_daySinceLastRestore']>30: fg.f['derived_daySinceLastRestoreGT30']=1
        elif fg.f['activation_daySinceLastRestore']>14: fg.f['derived_daySinceLastRestore15to30']=1
        else: fg.f['derived_daySinceLastRestoreLT15']=1



        fg.f['derived_employerid']=fg.f['employment_employer']

        if fg.f['payroll_hourlyRate']>20: fg.f['derived_hourlyRateGT20']=1
        else: fg.f['derived_hourlyRateGT20']=0

        if fg.f['payroll_lastPayrollStatus']!=11: fg.f['derived_lastPayrollStatus11']=0
        else: fg.f['derived_lastPayrollStatus11']=1

        if fg.f['cs_nCredit']>0: fg.f['derived_hasCSCredit']=1
        else: fg.f['derived_hasCSCredit']=0

        if fg.f['employment_nEmployer']>5: fg.f['derived_nEmployerGT5']=1
        else: fg.f['derived_nEmployerGT5']=0

    @ah_datadog.datadog_timed(name="getAllFeatures", tags=["operation:restore"])
    def getAllFeatures(self,uid):

    #    print("Start calculating features for %d"%uid)

        activation = ActivationFeature(uid)
        bank = BankFeature_predict(uid)
        payroll = Payroll(uid)
        device = DeviceFeature(uid)
        user = UserFeature(uid)
        employment = EmploymentFeature(uid)
        cs = CSFeature(uid)
        new = NewFeature(uid)
        timesheet = TimeSheetFeature(uid)
#        if len(payroll.data) == 0 or len(activation.data)==0:
#            print("no payroll data or no activation data %d" % uid)

#            return None
#        else:

        predTime = datetime.utcnow() #+timedelta(hours=-4) datetime.now(eastern)

        fg = FeatureGenerator(uid, predTime)

        currentact={}
        currentact['RequestTime']=predTime
        currentact['IsFail']=None
        currentact['IsLoss']=None
        currentact['RestoreReturnCodeAch']=None
        currentact['Amount']=100
        currentact['RestoreDate']=predTime
        currentact['ActivationDate']=predTime
        currentact['activationid']=0


   #     print((predTime-activation.data[-1]['RequestTime']).total_seconds())

        if len(activation.data)>0 and (predTime-activation.data[-1]['RequestTime']).total_seconds()<60:
            currentact=activation.data.pop()

        fg.feature_currentActivation(currentact)

        fg.feature_generator(activation)
        fg.feature_generator(payroll)
        fg.feature_generator(device)
        fg.feature_generator(user)
        fg.feature_generator(employment)
        fg.feature_generator(cs)
        fg.feature_generator(bank)
        fg.feature_generator(new)
        fg.feature_generator(timesheet)

        self.createDerivedFeature(fg)
        return fg
    def getPredTimeTarget(self,activation,uid):

        restore={}

        for l in activation.data:

            rdate=l['RestoreDate']

            if l['ActFail']==0:
                if rdate not in restore: restore[rdate]=Restore(l)
                else: restore[rdate].update(l)



        predTimeTarget=[]

        rdates=sorted(restore.keys(),reverse=True)

      #  print(rdates)
        for i in range(len(rdates)):
            rdate=rdates[i]
            r=restore[rdate]
            predtime=rdate



            predTimeTarget.append([r.status,r.hasLoss,predtime])

        return predTimeTarget

    def createMaster(self, fout, uids):
        writer = None
        n = 1
        print("Start creating master data")
        print("Total number of user %d" % len(uids))
        for uid in uids:
            uid=int(uid)
            if n % 1000 == 0:
                print(n)
            n += 1
            while True:
                activation = ActivationFeature(uid, scoring=False)
                bank = BankFeature(uid)
                payroll = Payroll(uid)
                device = DeviceFeature(uid)
                user = UserFeature(uid)
                employment = EmploymentFeature(uid)
                cs = CSFeature(uid)
                # pip = PIPFeature(self.conn, uid)
                timesheet = TimeSheetFeature(uid)
                new = NewFeature(uid)
                predtimetarget = self.getPredTimeTarget(activation, uid)
                break

            # print(predtimetarget)
            for pt in predtimetarget:
                #         print (uid,pt)

                predTime = pt[2]

                if predTime > datetime(2015, 8, 1):
                    fg = FeatureGenerator(uid, predTime)

                    fg.f['IsFail']=pt[0]
                    fg.f['IsLoss']=pt[1]
                    try:
                        fg.feature_generator(activation)
                    except IndexError:
                        print(uid)
                        continue

                    fg.feature_generator(bank)
                    fg.feature_generator(payroll)
                    fg.feature_generator(device)
                    fg.feature_generator(user)
                    fg.feature_generator(employment)
                    fg.feature_generator(cs)
                    # fg.feature_generator(pip)
                    fg.feature_generator(timesheet)
                    fg.feature_generator(new)

              #      self.createDerivedFeature(fg)
                    if writer is None:
                        fieldnames = list(fg.f.keys())
                        writer = csv.DictWriter(fout, fieldnames=fieldnames)

                        writer.writeheader()

                    fg.printFeatures(writer)


def prepareTrainingData(uids):

    filepath = './modeling/model/restore/data/'
    filename = 'master_%d.csv'%os.getpid()

    while True:
        try:
            fout = open(filepath+filename, 'w', encoding='utf-8')
            break
        except IOError:
            os.mkdir(filepath)

    model = RestoreModel()

    #     #    print (fg.f)

    model.createMaster(fout,uids)

    fout.close()


if __name__ == "__main__":

    filepath = './modeling/model/restore/data/'

    cmd = 'rm -f ' + filepath + 'master*.csv'
    os.system(cmd)

    model = RestoreModel()
    from numpy import *

    uids = array(model.getAllUserId())
    print(len(uids))
    nprocess = 8


    uids=[[uids[i] for i in range(len(uids)) if i%nprocess==x] for x in range(nprocess)]

    for i in uids: print(len(i))
 #   print(uids[0])
  #  prepareTrainingData(uids[0])

    pool = Pool(processes=nprocess)
    pool.map(prepareTrainingData, uids)


# if __name__ == "__main__":
#     uid = [387208]
#     conn = connect_db(30)

#     filepath = './data/'
#     filename = 'master_%d.csv'
#     fout = open(filepath+filename, 'w', encoding='utf-8')
#     model = RestoreModel(conn)

#     model.createMaster(fout, uid)

#     fout.close()


    # filepath = './modeling/model/risk_model/activation_risk_model/data/'
    # filename = 'master.csv'

    # while True:
    #     try:
    #         fout = open(filepath+filename, 'w')
    #         break
    #     except IOError:
    #         os.mkdir(filepath)



    # model = ActivationRiskModel(conn)
    # #     #    print (fg.f)
    # model.createMaster(fout)

    # fout.close()

    ###########################################
    # connection = connect_db(30)
    # uids = [45563,
    #         221451,
    #         220509,
    #         217584,
    #         218629,
    #         227137,
    #         223211,
    #         222097,
    #         210520,
    #         83029,
    #         151098,
    #         192160,
    #         223137,
    #         212042,
    #         221115]
    # uids = [60768]
    # model = MaxAdjustmentModel(connection)
    # for uid in uids:
    #     print(uid)
    #     fg = model.getAllFeatures(uid)

    #     predictor = Predictor(fg.f)
    #     print(predictor.getScore())
    #     # print(predictor.getReasonCode())
    #     print(predictor.getReasonCategory())
    #############################################

    # #############################################
    # connection = connect_db(30)
    # sql = '''
    # SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
    # SELECT UserID
    # FROM Transactions
    # WHERE TransactionTypeID = '1' or TransactionTypeID = '9'
    # GROUP BY UserID
    # HAVING MAX(Date) > '2016-06-18'
    # '''
    # df = pd.read_sql(sql, con=connection)
    # #     cursor = connection.cursor(as_dict=True)
    # uids = df['UserID'].values
    # log.info('connected to DB')

    # model = MaxAdjustmentModel(connection)

    # f = open('activeuser_max_model.csv', 'w')
    # writer = csv.DictWriter(f, fieldnames=['UserId', 'score'])
    # writer.writeheader()

    # for uid in uids:
    #     print(uid)
    #     fg = model.getAllFeatures(str(uid))

    #     predictor = Predictor(fg.f)
    #     writer.writerow({'UserId': uid, 'score': predictor.getScore()})

    # f.close()
    # connnection.close()
    # #################################################

    # connection = connect_db(30)
    # sql = '''
    # SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
    # SELECT UserID
    # FROM Transactions
    # WHERE TransactionTypeID = '1' or TransactionTypeID = '9'
    # GROUP BY UserID
    # HAVING MAX(Date) >= '2016-07-01'
    # '''
    # df = pd.read_sql(sql, con=connection)
    # #     cursor = connection.cursor(as_dict=True)
    # uids = df['UserID'].values
    # N = df.shape[0]
    # log.info('connected to DB')

    # model = MaxAdjustmentModel(connection)

    # f = open('activeuser_max_reason_score.csv', 'w')
    # writer = csv.DictWriter(f, fieldnames=['Reason', 'score'])
    # writer.writeheader()

    # reasons = dict()
    # for uid in uids:
    #     try:
    #         print(uid)
    #         fg = model.getAllFeatures(str(uid))

    #         predictor = Predictor(fg.f)
    #         reason = predictor.getReasonCategory()
    #         if reasons:
    #             for res in reasons:
    #                 reasons[res] += reason[res]/N
    #             else:
    #                 reasons = reason
    #     except:
    #         pass

    # for res in reasons:
    #     writer.writerow({'Reason': res, 'score': reasons[res]/N})

    # f.close()
    # connnection.close()
