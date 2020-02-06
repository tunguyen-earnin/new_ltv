import ah_db


class DataGenerator:
    def data_activation(self, uid, startDate=None, endDate=None):
        sql='''
            SELECT userid,(case when IsRestoreFailureAch=1 or isRestoreFailureTransactionStatus=1 or recoverycount>0 then 1 else 0 end) as IsFail,IsLoss,  RestoreReturnCodeAch,RestoreReturnDate, CAST(requestdate AS DATETIME) + CAST(requesttime AS DATETIME) as RequestTime, case when requesttime<'06:00:00' then 1 else 0 end as IsNight,ActivationDate, IsFirstRestore, platform as OS,  (case when (isActivationFailureAch=1) or (IsCancelled=1) or (isError=1) then 1 else 0 end ) as ActFail, CAST(Amount as Float) as Amount, IsTipFailureTransactionStatus as IsTipFail,CAST(TipAmount as Float) as TipAmount, CAST(RestoreDate AS DATETIME) RestoreDate, IsLightningPay
            FROM Analysis.Activations
            where userid=%d and IsPending=0 and RestoreDate<DATEADD(week, -1, getdate())
            order by RequestTime
            '''
        return ah_db.execute_to_json('sqlserver', sql, uid)


if __name__ == "__main__":
    pass
