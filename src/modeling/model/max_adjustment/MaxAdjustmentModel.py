import ah_config
import ah_datadog
import ah_db
import csv
import os
import pandas as pd
import time
import traceback
from modeling.feature.feature_activation import ActivationFeature, Restore
from modeling.feature.feature_bank import BankFeature_predict, BankFeature
from modeling.feature.feature_payroll import Payroll
from modeling.feature.feature_device import DeviceFeature
from modeling.feature.feature_season import SeasonFeature
from modeling.feature.feature_user import UserFeature
from modeling.feature.feature_employment import EmploymentFeature
from modeling.feature.feature_cs import CSFeature
from modeling.feature.feature_timesheet import TimeSheetFeature
from datetime import datetime, timedelta
from modeling.feature.feature_generator import FeatureGenerator
from modeling.feature.feature_new import NewFeature
from modeling.feature.feature_currentMax import CurrentMaxFeature
from modeling.misc.misc import get_df
from multiprocessing import Pool


class MaxAdjustmentModel:
    def __init__(self):
        self.dummyVar=[]
        self.pkl_path = os.path.dirname(os.path.abspath(__file__))+'/'

    def getAllUserId(self):

        start_date = '2018-11-01'
        end_date = '2018-12-31'

        sql_money = '''
            SELECT distinct ft.userid
            FROM fundstransfers ft
            JOIN transferattempts ta
            ON ta.FundsTransferId = ft.FundsTransferId
            WHERE ft.FundsTransferReasonId = 2 
            AND ft.postingdate between '{0}' and '{1}'
            AND ta.FundsTransferStatusId in (5, 7)
            AND ta.TransferAttemptId = GetDefiningTransferAttemptId(ft.FundsTransferId)
            ORDER BY ft.userid
            '''.format(start_date, end_date)

        with ah_db.open_mysql_cursor('moneyMovement') as cursor:
            cursor.execute(sql_money)
            df_money = pd.DataFrame(cursor.fetchall())

        with ah_db.open_mysql_cursor('miscellaneous') as cursor:
            sql_misc = '''SELECT userid from testusers'''
            cursor.execute(sql_misc)
            df_misc = pd.DataFrame(cursor.fetchall())

        df = df_money.merge(df_misc, on='userid', how='left', indicator=True)
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

        if fg.f['payroll_hourlyRate'] is None:
            fg.f['derived_hourlyRateGT20'] = 0
        elif fg.f['payroll_hourlyRate'] > 20:
            fg.f['derived_hourlyRateGT20'] = 1
        else:
            fg.f['derived_hourlyRateGT20'] = 0

        if fg.f['payroll_lastPayrollStatus']!=11: fg.f['derived_lastPayrollStatus11']=0
        else: fg.f['derived_lastPayrollStatus11']=1

        if fg.f['cs_nCredit']>0: fg.f['derived_hasCSCredit']=1
        else: fg.f['derived_hasCSCredit']=0

        if fg.f['employment_nEmployer']>5: fg.f['derived_nEmployerGT5']=1
        else: fg.f['derived_nEmployerGT5']=0

    def createInterFeature(self, fg):

        fg.f['bank_employer'] = str(fg.f['bank_getInstitutionId']) + '_' + str(fg.f['employment_employer'])
        fg.f['inter_amtActCurrent_aveAmtAct_Wk12'] = fg.f['activation_amtActCurrent'] / (
                fg.f['activation_aveAmtAct_Wk12'] + 0.01)
        fg.f['inter_amtActCurrent_aveAmtAct_Wk2'] = fg.f['activation_amtActCurrent'] / (
                fg.f['activation_aveAmtAct_Wk2'] + 0.01)
        fg.f['inter_amtActCurrent_aveAmtAct_trend'] = fg.f['inter_amtActCurrent_aveAmtAct_Wk2'] / (
                fg.f['inter_amtActCurrent_aveAmtAct_Wk12'] + 0.01)
        del fg.f['inter_amtActCurrent_aveAmtAct_Wk12']
        fg.f['inter_weekly_act_cnt_Wk2'] = fg.f['activation_amtActCurrent'] / (
                    fg.f['activation_aveAmtPerAct_Wk2'] + 0.01)
        fg.f['inter_AmtPerAct_Wk2'] = fg.f['activation_amtActCurrent'] / (fg.f['activation_aveCntAct_Wk2'] + 0.01)
        fg.f['inter_amtActCurrent_aveEarning'] = fg.f['activation_amtActCurrent'] / (fg.f[
                                                                                         'bank_avgAmountPerEarningWithinNWeeksN_2'] + 0.01)

    @ah_datadog.datadog_timed(name="getAllFeatures", tags=["operation:maxAdjustment"])
    def getAllFeatures(self,uid):

    #    print("Start calculating features for %d"%uid)
        log = ah_config.getLogger('ah.max_adjustment_model')
        start = time.time()
        activation = ActivationFeature(uid)
        end = time.time()
        log.debug('Time elapsed for activation feature queries: '
                  '%d sec for user %s', end-start, uid)
        start = end

        bank = BankFeature_predict(uid)
        end = time.time()
        log.debug('Time elapsed for bank feature queries: '
                  '%d sec for user %s', end-start, uid)
        start = end

        payroll = Payroll(uid)
        end = time.time()
        log.debug('Time elapsed for payroll feature queries: '
                  '%d sec for user %s', end-start, uid)
        start = end

        device = DeviceFeature(uid)
        end = time.time()
        log.debug('Time elapsed for device feature queries: '
                  '%d sec for user %s', end-start, uid)
        start = end

        user = UserFeature(uid)
        end = time.time()
        log.debug('Time elapsed for user feature queries: '
                  '%d sec for user %s', end-start, uid)
        start = end

        employment = EmploymentFeature(uid)
        end = time.time()
        log.debug('Time elapsed for employment feature queries: '
                  '%d sec for user %s', end-start, uid)
        start = end

        cs = CSFeature(uid)
        end = time.time()
        log.debug('Time elapsed for CS feature queries: '
                  '%d sec for user %s', end-start, uid)
        start = end

        #pip = PIPFeature(uid)
        #end = time.time()
        #log.debug('Time elapsed for PIP feature queries: '
        #          '%d sec for user %s', end-start, uid)
        #start = end

        timesheet = TimeSheetFeature(uid)
        end = time.time()
        log.debug('Time elapsed for timesheet feature queries: '
                  '%d sec for user %s', end-start, uid)
        start = end

        new = NewFeature(uid)
        end = time.time()
        log.debug('Time elapsed for new feature queries: '
                  '%d sec for user %s', end-start, uid)
        start = end

        currentMax = CurrentMaxFeature(uid)
        end = time.time()
        log.debug('Time elapsed for max feature queries: '
                  '%d sec for user %s', end - start, uid)
        start = end

        season = SeasonFeature()
        end = time.time()
        log.debug('Time elapsed for season feature queries: '
                  '%d sec', end - start)

#        if len(payroll.data) == 0 or len(activation.data)==0:
#            print("no payroll data or no activation data %d" % uid)

#            return None
#        else:

        predTime = datetime.utcnow() #+ timedelta(hours=-4)
        # datetime.now(eastern)

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
        fg.feature_generator(season)
        fg.feature_generator(user)
        fg.feature_generator(employment)
        fg.feature_generator(cs)
        fg.feature_generator(bank)
        # fg.feature_generator(pip)
        fg.feature_generator(timesheet)
        fg.feature_generator(new)
        fg.feature_generator(currentMax)
        self.createDerivedFeature(fg)
        self.createInterFeature(fg)
        return fg

    def get_pay_dates(self, uid):
        sql = '''SELECT  distinct CAST(paydate AS DATETIME)  as paydate 
        FROM Payroll.PayrollActiveStatusTracking 
        Where UserId = {} order by paydate'''.format(uid)
        rows = ah_db.execute_to_json('payroll', sql)
        return [l['paydate'] for l in rows]

    def getPredTimeTarget(self,activation,uid):

        restore={}

        for l in activation.data:

            rdate=l['RestoreDate']

            if l['ActFail']==0:
                if rdate not in restore: restore[rdate]=Restore(l)
                else: restore[rdate].update(l)

        paydates = self.get_pay_dates(uid)

        predTimeTarget=[]

        rdates=sorted(restore.keys(),reverse=True)

      #  print(rdates)
        for i in range(0,len(rdates)-2):
            rdate=rdates[i]
            r=restore[rdate]
            predtime=rdate-timedelta(7)
            while len(paydates)>0 and paydates[-1]>=rdate:
                paydates.pop()
            if len(paydates)>0:
                if (rdate-paydates[-1]).days<35 and paydates[-1]<predtime:
                    predtime=paydates[-1]
        #            print(predtime)

            if rdates[i+1]>predtime: predtime=rdates[i+1]

          #  print(predtime)
            predtime=predtime-timedelta(2)
            if predtime<rdates[i+1]:
                predTimeTarget.append([max(restore[rdates[i+1]].status,r.status),max(restore[rdates[i+1]].hasLoss,r.hasLoss),predtime])
            else:
                predTimeTarget.append([r.status, r.hasLoss, predtime])

        return predTimeTarget

    def createMaster(self, fout, uids):

        writer = None
        n = 1
        print("Start creating master data")
        print("Total number of user %d" % len(uids))
        # for uid in uids:
        while len(uids) > 0:
            if n % 1000 == 0:
                print(n)
            n += 1
            try:
                uid = uids.pop()
                print('uid=' + str(uid))
                uid = int(uid)
                activation = ActivationFeature(uid,scoring=False)
                bank = BankFeature(uid)
                payroll = Payroll(uid)
                device = DeviceFeature(uid)
                user = UserFeature(uid)
                employment = EmploymentFeature(uid)
                cs = CSFeature(uid)
                # pip = PIPFeature(uid)
                timesheet = TimeSheetFeature(uid)
                new = NewFeature(uid)
                currentMax = CurrentMaxFeature(uid)
                season = SeasonFeature()
                #print("initialized feature objects for user:{}".format(uid))
                # break
                    # if len(payroll.data) == 0:
                    #     print("no payroll data %d" % uid)
                    #     continue

                    # print(uid, datetime.now())
                    # self.conn.close()
                    # self.conn = connect_db(30)
                    # print('Datebase reconnected!')

                predtimetarget=self.getPredTimeTarget(activation,uid)
            except:
                print(uid, traceback.format_exc())
                uids.append(uid)
                return uids


            for pt in predtimetarget:
       #         print (uid,pt)

                predTime = pt[2]

                if predTime > datetime(2017, 10, 31):

                    fg = FeatureGenerator(uid, predTime)

                    fg.f['IsFail']=pt[0]
                    fg.f['IsLoss']=pt[1]
                    fg.feature_generator(activation)
                    fg.feature_generator(bank)
                    fg.feature_generator(payroll)
                    fg.feature_generator(device)
                    fg.feature_generator(season)
                    fg.feature_generator(user)
                    fg.feature_generator(employment)
                    fg.feature_generator(cs)
                    # fg.feature_generator(pip)
                    fg.feature_generator(timesheet)
                    fg.feature_generator(new)
                    fg.feature_generator(currentMax)
                    try:
                        self.createDerivedFeature(fg)
                        self.createInterFeature(fg)
                    except:
                        print(uid)
                        print(traceback.format_exc())
                    if writer is None:
                        fieldnames = list(fg.f.keys())
                        writer = csv.DictWriter(fout, fieldnames=fieldnames)

                        writer.writeheader()

                    fg.printFeatures(writer)

        return uids

    def testScore(self):
        filepath = './modeling/model/risk_model/activation_risk_model/log/'
        filename = 'score_tmp.csv'

        while True:
            try:
                fout = open(filepath+filename, 'w')
                break
            except IOError:
                os.mkdir(filepath)

        writer = csv.writer(fout, delimiter=',')
        writer.writerow(['UserId', 'Risk Score'])
        model = ActivationRiskModel()
        uids = model.getAllUserIdMaxAdjust()
        #    uids = [23528]
        for uid in uids:
            try:
                fg = model.getAllFeatures(uid)

                try:
                    score = model.getScore(fg.f)
                except:
                    score = -1
            except:
                score = -10

            writer.writerow([uid, score])

            print(score)
            model.__writeDB__(fg.f)


def prepareTrainingData(uids):

    filepath = './modeling/model/risk_model/max_adjustment_model/data/'
    filename = 'master_%d.csv'%os.getpid()

    while True:
        try:
            fout = open(filepath+filename, 'a+', encoding='utf-8')
            break
        except IOError:
            os.mkdir(filepath)

    model = MaxAdjustmentModel()
    while len(uids) > 0:
        try:
            uids = model.createMaster(fout, uids)
        except:
            print(datetime.now(), "restarted data generation")

    fout.close()


def test(uid):
    log = ah_config.getLogger('ah.max_adjustment_model')
    log.info('connected to DB')
    import modeling.misc.predictor as pred
    ah_config.initialize()
    model = MaxAdjustmentModel()
    fg = model.getAllFeatures(uid)
    predictor = pred.Predictor(fg.f)
    predictor.getReasonCategory()
    return 1


if __name__ == "__main__":
    filepath = './modeling/model/max_adjustment/data/'
    useridfile = 'alluserid.csv'

    ah_config.initialize()
    import os
    cmd = 'rm -f ' + filepath + 'master*.csv'
    os.system(cmd)

    model = MaxAdjustmentModel()
    uids = 8726
    fg = model.getAllFeatures(uids)
    print(fg.f)

    '''
    from numpy import *
    uids = model.getAllUserId()
    print(len(uids))
    from random import randint

    df = pd.DataFrame(uids)
    df.to_csv(filepath + useridfile)

    uids = pd.read_csv(filepath + useridfile)['0'].tolist()

    nprocess=8

    uids = [[uids[i] for i in range(len(uids)) if i % nprocess == x] for x in range(nprocess)]
    for i in uids: print(len(i))

    pool = Pool(processes=nprocess)
    uids = pool.map(prepareTrainingData, uids)
    '''



    # filename = 'master_*.csv'
    # cmd = ''
    # cmd = 'cat ' + filepath + filename + ' > '
    # os.system()


    # conn = connect_db(30)

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
