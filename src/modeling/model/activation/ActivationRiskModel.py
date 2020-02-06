import csv
from datetime import datetime, timedelta
from modeling.feature.feature_activation import ActivationFeature
from modeling.feature.feature_bank import BankFeature_predict, BankFeature
from modeling.feature.feature_payroll import Payroll
from modeling.feature.feature_device import DeviceFeature
from modeling.feature.feature_user import UserFeature
from modeling.feature.feature_employment import EmploymentFeature
from modeling.feature.feature_cs import CSFeature
from modeling.feature.feature_generator import FeatureGenerator
from modeling.feature.feature_timesheet import TimeSheetFeature
from modeling.feature.feature_new import NewFeature
from modeling.misc.misc import get_df
import traceback
import os
import ah_datadog


class ActivationRiskModel:
    def __init__(self):
        self.dummyVar = []
        self.pkl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "activation_lr.pkl")

    def getAllUserId(self):
        startdate = '2018-01-01'
        enddate = '2018-03-31'
        sql1 = '''
        SELECT  distinct ft.userid
        FROM fundstransfers ft
        JOIN transferattempts ta on ta.FundsTransferId = ft.FundsTransferId
        WHERE ft.FundsTransferReasonId=2 and ft.postingdate between '{0}' and '{1}'
        AND ta.FundsTransferStatusId in (5,7) 
        AND ta.TransferAttemptId = GetDefiningTransferAttemptId(ft.FundsTransferId)
        ORDER BY ft.userid desc
        '''.format(startdate, enddate)
        df1 = get_df('moneyMovement', sql1)

        sql2 = '''
         SELECT userid from testusers
         '''
        df2 = get_df('miscellaneous', sql2)

        df = df1.merge(df2, on='userid', how='left', indicator=True)
        df = df['userid'][df['_merge'] == 'left_only']
        return [l for l in df]


    def createDerivedFeature(self, fg):
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

    @ah_datadog.datadog_timed(name="getAllFeatures", tags=["operation:activation"])
    def getAllFeatures(self, uid):

    #    print("Start calculating features for %d"%uid)


        activation = ActivationFeature(uid)
        bank = BankFeature_predict(uid)
        payroll = Payroll(uid)
        device = DeviceFeature(uid)
        user = UserFeature(uid)
        employment = EmploymentFeature(uid)
        cs = CSFeature(uid)
        # pip = PIPFeature(uid)
        timesheet = TimeSheetFeature(uid)
        #        if len(payroll.data) == 0 or len(activation.data)==0:
        #            print("no payroll data or no activation data %d" % uid)

        #            return None
        #        else:
        new = NewFeature(uid)

        predTime = datetime.utcnow() +timedelta(hours=-5)# datetime.now(eastern)
        # predTime = datetime.utcnow() +timedelta(days=2)
        fg = FeatureGenerator(uid, predTime)

        currentact = {}
        currentact['RequestTime'] = predTime
        currentact['IsFail'] = None
        currentact['IsLoss'] = None
        currentact['RestoreReturnCodeAch'] = None
        currentact['Amount'] = 100

        currentact['RestoreDate'] = predTime
        currentact['ActivationDate'] = predTime
        currentact['activationid'] = 0

   #     print((predTime-activation.data[-1]['RequestTime']).total_seconds())
        if len(activation.data) > 0:
            currentact['RestoreDate'] = activation.data[-1]['RestoreDate']
            if (predTime-activation.data[-1]['RequestTime']).total_seconds() < 3600:
                currentact = activation.data.pop()


        fg.feature_currentActivation(currentact)

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


    def refineTarget(self, all):

        restore={}
        for c in all:
            if c['RestoreDate'] not in restore:
                restore[c['RestoreDate']]=0
            if c['IsFail']==1:
                restore[c['RestoreDate']]=1

        for c in all:
            c['IsFail_Refine']=restore[c['RestoreDate']]


    def createMaster(self, fout, uids):

        writer = None
        n = 1
        print("Start creating master data")
        print("Total number of user %d" % len(uids))
        for uid in uids:

            uid = int(uid)
            print('uid=' + str(uid))
            if n % 100 == 0:
                print(n)
            n += 1
            while True:
                try:
                    activation = ActivationFeature(uid)
                    bank = BankFeature(uid)
                    payroll = Payroll(uid)
                    device = DeviceFeature(uid)
                    user = UserFeature(uid)
                    employment = EmploymentFeature(uid)
                    cs = CSFeature(uid)
                    #pip = PIPFeature(uid)
                    timesheet = TimeSheetFeature(uid)
                    new = NewFeature(uid)
                    break
                except:
                    traceback.print_exc()

            if len(payroll.data) == 0:
                print("no payroll data %d" % uid)
                continue
            self.refineTarget(activation.data)

            while len(activation.data) > 0:
                #   for i in range(1, len(data)):

                current = activation.data.pop()
                predTime = current['RequestTime']

                if predTime > datetime(2017, 1, 1) and current['ActFail']==0:
                    fg = FeatureGenerator(uid, predTime)
                    fg.feature_currentActivation(current)
                    #fg.f['IsFail_Refine']=current['IsFail_Refine']
                    fg.feature_generator(activation)
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
                        fieldnames = list(fg.f.keys())
                        writer = csv.DictWriter(fout, fieldnames=fieldnames)

                        writer.writeheader()

                    fg.printFeatures(writer)



def prepareTrainingData(uids):

    filepath = './modeling/model/activation_risk/data/'
    filename = 'master_%d.csv' % os.getpid()
    os.makedirs(filepath, exist_ok=False)
    fout = open(filepath+filename, 'w', encoding='utf-8')

    model = ActivationRiskModel()
    model.createMaster(fout, uids)

    fout.close()



if __name__ == "__main__":
    print("Healthy")
    from modeling.misc.misc import getSQLdata, getESdata, getESdataForPayroll
    import pymssql, os, copy
    from line_profiler import LineProfiler
#
#     conn = connect_db(30)
#     print("everything is normal")
#     model = ActivationRiskModel(conn)
#     userid = 138113
#
#     ft = model.getAllFeatures(userid)
# #     # fg, t1, t2, t3, t4 = model.getAllFeatures(45563)
#
#     lp = LineProfiler()
#     lp_wrapper = lp(model.getAllFeatures)
#     lp_wrapper(userid)
#     lp.print_stats()
#
#     User = pd.read_csv("UserId.csv")
#
#     total_time = []
#     for uid in User.UserId :
#         fg, eclipsed_time = model.getAllFeatures(int(uid))
#         total_time.append(eclipsed_time)
#
#     User["total_time2"] = total_time
    #
    # time1 = []
    # time2 = []
    # time3 = []
    # time4 = []
    #
    # for uid in User.UserId:
    #     print(uid)
    #     fg, t1, t2, t3, t4 = model.getAllFeatures(int(uid))
    #     time1.append(t1)
    #     time2.append(t2)
    #     time3.append(t3)
    #     time4.append(t4)
    #
    # User["time1"] = time1
    # User["time2"] = time2
    # User["time3"] = time3
    # User["time4"] = time4

 #    #     #    print (fg.f)
 #    from numpy import *
 #    uids = array(model.getAllUserId())
 #    print(len(uids))
 #    # prepareTrainingData(uids)
 #    from random import randint
 #    nprocess = 8
 #    group=array([randint(0,nprocess-1) for i in range(len(uids))])
 #
 #    uids=[ list(uids[group==x]) for x in range(nprocess)]
 #
 #    for i in uids: print(len(i))
 # #   print(uids[0])
 #  #  prepareTrainingData(uids[0])
 #
 #    pool = Pool(processes=nprocess)
 #    pool.map(prepareTrainingData, uids)


    # connection = connect_db(30)
    # # uids = [45563,
    # #         221451,
    # #         220509,
    # #         217584,
    # #         218629,
    # #         227137,
    # #         223211,
    # #         222097,
    # #         210520,
    # #         83029,
    # #         151098,
    # #         192160,
    # #         223137,
    # #         212042,
    # #         221115]

    # uids = [232129]

    # model = ActivationRiskModel(connection)
    # for uid in uids:
    #     print(uid)
    #     fg = model.getAllFeatures(uid)

    #     predictor = Predictor(fg.f)
    #     print(predictor.getScore())
    #     print(predictor.getReasonCode())
    #
    #
    # """
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
    #
    # model = ActivationRiskModel(connection)
    #
    # # fg = model.getAllFeatures(197452)
    #
    # # predictor = Predictor(fg.f)
    # # print(predictor.getScore())
    #
    # f = open('activeuser_activation_model.csv', 'w')
    # writer = csv.DictWriter(f, fieldnames=['UserId', 'score'])
    # writer.writeheader()
    #
    # for uid in uids:
    #     print(uid)
    #     fg = model.getAllFeatures(str(uid))
    #
    #     predictor = Predictor(fg.f)
    #
    #     writer.writerow({'UserId': uid, 'score': predictor.getScore()})
    #
    #     # try:
    #     #     fg = model.getAllFeatures(str(uid))
    #
    #     #     predictor = Predictor(fg.f)
    #
    #     #     writer.writerow({'UserId': uid, 'score': predictor.getScore()})
    #     # except:
    #     #     pass
    # f.close()
    # connection.close()
    # """

