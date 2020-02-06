import ah_db
import sys
import traceback
from utils import calcRate
from modeling.feature.feature_generic import FeatureGeneric
import numpy as np
from datetime import datetime, timedelta


class Restore:
    def __init__(self, l):
        self.amt = l['Amount']
        self.rdate = l['RestoreDate']
        self.status = l['IsFail']
        self.hasLoss = l['IsLoss']
        self.tipStatus = l['IsTipFail']
        self.achReturnDate = l['RestoreReturnDate']
        self.restoreReturnCodeAch = l['RestoreReturnCodeAch']
        self.nAct = 1

    def update(self, l):
        self.amt += l['Amount']
        self.nAct += 1
        if l['IsFail'] == 1:
            self.status = 1
            if l['RestoreReturnDate'] is not None and \
               (self.achReturnDate is None or
               self.achReturnDate > l['RestoreReturnDate']):
                self.achReturnDate = l['RestoreReturnDate']
        if l['IsTipFail'] == 1:
            self.tipStatus = 1

        if l['IsLoss'] == 1:
            self.hasLoss = 1

class ActivationFeature(FeatureGeneric):
    nWkMax = 20

    def __init__(self, uid, scoring=True):
        self.scoring = scoring
        FeatureGeneric.__init__(self, uid)

        self.decayFactor = 1 - np.exp(-1.0/5)

    def initMember(self):
        self.firstActDate = None

        self.tipRateExp = 0
        self.amtExp = -1
        self.tipExp = -1

        self.nAct = np.zeros(ActivationFeature.nWkMax)
        self.nActFail = np.zeros(ActivationFeature.nWkMax)

        self.amtAct = np.zeros(ActivationFeature.nWkMax)
        self.nLP = np.zeros(ActivationFeature.nWkMax)
        self.tip = np.zeros(ActivationFeature.nWkMax)
        self.nFailRestore = np.zeros(ActivationFeature.nWkMax)
        self.nFailTip = np.zeros(ActivationFeature.nWkMax)
        self.nNightAct = np.zeros(ActivationFeature.nWkMax)
        self.amtRestore = np.zeros(ActivationFeature.nWkMax)

        self.nRestore = np.zeros(ActivationFeature.nWkMax)
        self.nRestoreAct = np.zeros(ActivationFeature.nWkMax)
        self.nAct56 = np.zeros(ActivationFeature.nWkMax)

        self.restore = {}  # restoredate to restore
        #     self.daysToNextRestore
        self.amtFutureRestore = 0
        self.nFutureRestoreAct = 0
        self.daySinceLastRestore = 7*ActivationFeature.nWkMax
        self.daySicceLastFailRestore = 7*ActivationFeature.nWkMax
        self.daySinceLastAct = 7*ActivationFeature.nWkMax
        self.amtPreRestore = 0
        self.firstRestoreDate = None
        self.maxActGapWk = 0

    def getDataRangeIndex(self, predTime):
        r = 0
        for i in range(len(self.data)-1, 0, -1):
            if self.data[i]['RequestTime'] is not None and \
               predTime > self.data[i]['RequestTime']:
                r = i
                break
        return r

    def parseData(self, predTime):
        # assert data[-1]['RequestTime']<predTime
        # assert len(data)>0
        lastIndex = self.getDataRangeIndex(predTime)
        data = self.data[:(lastIndex+1)]

        lastwk = -1

        if len(data) > 0:
            self.firstActDate = data[0]['RequestTime']
            daySinceLastAct_tmp = (predTime-data[-1]['RequestTime'])\
                .total_seconds()/86400
            self.daySinceLastAct = [self.daySinceLastAct,
                                    daySinceLastAct_tmp][self.daySinceLastAct >
                                                         daySinceLastAct_tmp]
        else:
            self.firstActDate = predTime
            self.firstRestoreDate = predTime + timedelta(days=1)
            self.daySinceLastRestore = -1

        for l in data:
            l['Amount'] = float(l['Amount'])
            l['TipAmount'] = float(l['TipAmount'])
            self.firstRestoreDate = self.firstRestoreDate or l['RestoreDate']
            wk = int((predTime - l['RequestTime']).days/7)
            if lastwk - wk > self.maxActGapWk:
                self.maxActGapWk = lastwk - wk
            lastwk = wk

            # print(wk,l['RequestTime'])

            if wk < ActivationFeature.nWkMax:
                rdate = l['RestoreDate']

                if l['ActFail'] == 0:
                    if self.amtExp < 0:
                        self.amtExp = l['Amount']
                    else:
                        self.amtExp = (1 - self.decayFactor) * self.amtExp +\
                                      self.decayFactor*l['Amount']

                    if self.tipExp < 0:
                        self.tipExp = l['TipAmount']
                    else:
                        self.tipExp = (1 - self.decayFactor) * self.tipExp +\
                            self.decayFactor*l['TipAmount']
                    #     print(self.tipExp,self.amtExp,self.decayFactor)

                    self.nAct[wk] += 1

                    self.amtAct[wk] += l['Amount']
                    if l['IsLightningPay'] == 1:
                        self.nLP[wk] += 1
                    self.tip[wk] += l['TipAmount']

                    if rdate not in self.restore:
                        self.restore[rdate] = Restore(l)
                    else:
                        self.restore[rdate].update(l)

                    if l['IsNight'] == 1:
                        self.nNightAct[wk] += 1
                    if l['RequestTime'].weekday() in [5, 6]:
                        self.nAct56[wk] += 1
                else:
                    self.nActFail[wk] += 1
                    #   print (self.daySinceLastAct)

        for rdate in self.restore:
            try:
                if rdate < predTime:
                    wk = int((predTime-rdate).days/7)
                    if wk < ActivationFeature.nWkMax:
                        daysincerestore = (predTime - rdate).days
                        if self.restore[rdate].status == 1 \
                           and (self.restore[rdate].achReturnDate is None or
                                self.restore[rdate].achReturnDate < predTime):
                            # only look at failures that is known before prediciton time
                            self.nFailRestore[wk] += 1
                            self.nFailTip[wk] += 1

                            self.daySicceLastFailRestore = \
                                [self.daySicceLastFailRestore,
                                 daysincerestore][daysincerestore <
                                                  self.daySicceLastFailRestore]
                        self.amtRestore[wk] += float(self.restore[rdate].amt)
                        self.nRestore[wk] += 1
                        self.nRestoreAct[wk] += self.restore[rdate].nAct

                        if daysincerestore < self.daySinceLastRestore:
                            self.daySinceLastRestore = daysincerestore
                            self.amtPreRestore = self.restore[rdate].amt
                else:
                    self.amtFutureRestore += self.restore[rdate].amt
                    self.nFutureRestoreAct += self.restore[rdate].nAct
            except:
                print(rdate, predTime, wk, self.restore[rdate].achReturnDate)
                traceback.print_exc()
                sys.exit()

    def buildFeatures(self, predTime):
        f = {}
        self.initMember()
        self.parseData(predTime)

        f['wkSinceFirstAct'] = (predTime - self.firstActDate).days/7+1
        #     print(self.uid,predTime,self.firstActDate)

        if self.firstRestoreDate is None or\
           predTime < self.firstRestoreDate:
            f['firstRestore'] = 1
        else:
            f['firstRestore'] = 0

        #      print(self.tipExp,self.amtExp)


        f['tipRateExp'] = calcRate(self.tipExp, self.amtExp)

        nwk = [2, 12]

        for n in nwk:
            # assert n<=Activations.nWkMax
            suffix = str(n)
            n = max(min(n, int(f['wkSinceFirstAct'])), 1)
            f['aveCntAct_Wk' + suffix] = sum(self.nAct[:n])/n
            f['aveCntActCancel_Wk'+suffix] = sum(self.nActFail[:n])/n
            f['cancelRate_Wk'+suffix] = calcRate(f['aveCntActCancel_Wk' +
                                                   suffix],
                                                 f['aveCntAct_Wk'+suffix])

            f['pctLP_Wk'+suffix] = calcRate(sum(self.nLP[:n])/n,
                                            f['aveCntAct_Wk'+suffix])

            f['aveAmtAct_Wk'+suffix] = sum(self.amtAct[:n])/n
            f['aveAmtPerAct_Wk'+suffix] = calcRate(f['aveAmtAct_Wk'+suffix],
                                                   f['aveCntAct_Wk'+suffix])
            f['aveTip_Wk'+suffix] = sum(self.tip[:n])/n
            f['tipRate_Wk'+suffix] = calcRate(f['aveTip_Wk'+suffix],
                                              f['aveAmtAct_Wk'+suffix])

            f['nFailedRestore_Wk'+suffix] = sum(self.nFailRestore[:n])/n
            f['nFailedTip_Wk'+suffix] = sum(self.nFailTip[:n])/n

            f['nFailedTip_Wk'+suffix] = sum(self.nFailTip[:n])/n
            f['amtPerRestore_Wk'+suffix] = calcRate(sum(self.amtRestore[:n]),
                                                    sum(self.nRestore[:n]))
            f['cntPerRestore_Wk'+suffix] = calcRate(sum(self.nRestoreAct[:n]),
                                                    sum(self.nRestore[:n]))

            f['pctAct56_Wk'+suffix] = calcRate(sum(self.nAct56[:n])/n,
                                               f['aveCntAct_Wk'+suffix])
            f['pctActNight_Wk'+suffix] = calcRate(sum(self.nNightAct[:n])/n,
                                                  f['aveCntAct_Wk'+suffix])

            f['aveDayGapAct_Wk'+suffix] = calcRate(7,
                                                   f['aveCntAct_Wk'+suffix])

        #   print(f.keys())
        varname = list(f.keys())

        for k in varname:
            if '_Wk2' in k:
                name = '_'.join(k.split('_')[:-1])
                k = 'trend'+name
                f[k] = calcRate(f[name+'_Wk2'], f[name+'_Wk12'])

        f['nActCurrent'] = self.nFutureRestoreAct
        f['amtActCurrent'] = self.amtFutureRestore
        # print (predTime,f['amtActCurrent'])
        f['daySinceLastRestore'] = self.daySinceLastRestore
        f['daySinceLastFailRestore'] = self.daySicceLastFailRestore
        # cap at 140
        f['daySinceLastAct'] = self.daySinceLastAct

        f['preRestoreAmt'] = self.amtPreRestore

        self.reName(f, 'activation_')

        return f

    def getData(self):
        sql = '''
        SELECT DISTINCT da.ActivationId
        , MAX(da.UserId) UserId
        , MAX((CASE WHEN (ta.FundsTransferStatusId = 5 AND tt.TypeName = 'Restore') OR (biiad.ReturnReasonCode LIKE 'R%%' AND tt.TypeName = 'Restore') THEN 1 ELSE 0 END)) IsFail
        , COALESCE(MIN(CASE ##WHEN (tt.TypeName = 'Recovery' AND ReturnReasonCode IS NULL AND restorestatus.StatusName = 'Pass') THEN 0
        WHEN ((tt.TypeName IN ('Restore', 'Recovery') AND ReturnReasonCode IS NOT NULL) OR (tt.TypeName IN ('Restore', 'Recovery') AND ta.FundsTransferStatusId <> 7)) THEN 1
        WHEN (tt.TypeName = 'Recovery' AND ReturnReasonCode IS NULL AND ta.FundsTransferStatusId = 7) THEN 0 ELSE NULL END), 0) IsLoss
        , MAX(biiad.DateReturned) RestoreReturnDate
        , MAX(da.ActivationDate) ActivationDate
        , MAX(CASE WHEN RestoreNumber.UserRestoreNumber = 0 THEN 1 ELSE 0 END) IsFirstRestore
        , MAX((CASE WHEN (ta.FundsTransferStatusId = 5 AND tt.TypeName IN ('Activation', 'InstantActivation')) OR (biiad.ReturnReasonCode LIKE 'R%%' AND tt.TypeName IN ('Activation', 'InstantActivation')) THEN 1 ELSE 0 END)) AS ActFail
        , MAX(da.CashoutAmount/100) Amount
        , MAX(CASE WHEN tip.FundsTransferStatusId = 5 THEN 1 ELSE 0 END) IsTipFail
        , MAX(CAST(da.Tip AS decimal(18,4))) TipAmount
        , MAX(CASE WHEN tt.TypeName = 'InstantActivation' THEN 1 ELSE 0 END) IsLightningPay
        , MAX(da.CreatedOn) RequestTime
        , MAX(CASE WHEN CAST(da.CreatedOn AS time)<'06:00:00' THEN 1 ELSE 0 END) AS IsNight
        , MAX(CAST(da.RestoreDate AS DATETIME)) RestoreDate
        , MAX(CASE WHEN tt.TypeName = 'Restore' THEN biiad.ReturnReasonCode ELSE NULL END) RestoreReturnCodeAch
        , MAX(da.ActivationOriginId) ActivationOriginId
        FROM Activations da
        LEFT JOIN transactions dt
        ON dt.ActivationID = da.ActivationID
        LEFT JOIN TransactionsToFundsTransfers mmttft
        ON dt.TransactionID = mmttft.TransactionId
        LEFT JOIN TransferAttempts ta 
        ON mmttft.FundsTransferId = ta.FundsTransferId
        LEFT JOIN InboundAchDetails biiad
        ON biiad.FundsTransferId = mmttft.FundsTransferId
        LEFT JOIN TransactionTypes tt
        ON tt.TransactionTypeID = dt.TransactionTypeID
        LEFT JOIN HourStatuses hs
        ON hs.HourStatusID = da.HourStatusID
        LEFT JOIN 
            (SELECT distinct t.ActivationId, ta.FundsTransferStatusId
             FROM Transactions t
             JOIN TransactionsToFundsTransfers m
             ON t.TransactionID = m.TransactionId
             JOIN TransferAttempts ta
             ON m.FundsTransferId = ta.FundsTransferId
             WHERE t.TransactionTypeID = 2 AND t.UserId={0}
             ) tip
            ON tip.ActivationId = da.ActivationID
        LEFT JOIN (
            SELECT a.UserID, a.RestoreDate, COUNT(b.RestoreDate) UserRestoreNumber
            FROM (
            SELECT DISTINCT a.UserID, CAST(a.RestoreDate AS Date) RestoreDate
            from Activations a
            where a.UserId={0}
            ) a
            LEFT JOIN (
            SELECT DISTINCT a.UserID, CAST(a.RestoreDate AS Date) RestoreDate
            from Activations a
            where a.UserId={0}
            ) b
            ON b.UserID = a.UserID AND b.RestoreDate < a.RestoreDate
            where a.UserId={0}
            GROUP BY a.UserID, a.RestoreDate
        ) RestoreNumber
        ON (RestoreNumber.UserID = da.UserID AND CAST(da.RestoreDate AS DATE) = RestoreNumber.RestoreDate)
        WHERE da.UserId={0}
        AND da.RestoreDate <= DATE_ADD(now(), interval -7 day) 
        GROUP BY da.ActivationId
        ORDER BY MAX(da.CreatedOn)
        '''.format(self.uid)

        return ah_db.execute_to_json('moneyMovement', sql)


if __name__ == "__main__":

    from datetime import datetime, timedelta
    import pandas as pd
    import ah_config
    ah_config.initialize()
    # from line_profiler import LineProfiler

    af = ActivationFeature(uid)
    ft = af.buildFeatures(date)

#     end = time.clock()
#
#     elapsed = end - start
#     print("elapsed time:", elapsed)

