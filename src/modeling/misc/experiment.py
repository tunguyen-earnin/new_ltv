import ah_config
import ah_db
import random
import numpy as np
from datetime import datetime
# from services.infrastructure.ioerror import ServiceUnavailable
from logging import getLogger
import traceback


class UserExperiment:
    def __init__(self, conn=None):
        self.conn = conn

    def getAllCurrentUXExperiment(self):
        sql = """
              SELECT ExperimentId
              FROM DataScience.Experiment
              WHERE UXExperiment = 1
              AND CURDATE() BETWEEN StartTime AND EndTime
              """

        #print('getAllCurrentUXExperiment '+str(time()))
        with ah_db.open_db_connection('datascience') as connection:
            result = connection.execute(sql)
            row = result.fetchall()
        return [x['ExerimentId'] for x in row]

    def getExperimentDateRange(self, eid):
        sql = """
              SELECT COUNT(*) AS cntExp
              FROM DataScience.Experiment
              WHERE experimentid={}
              AND CURDATE() BETWEEN StartTime AND EndTime
              """.format(eid)

        #print('getExperimentDateRange ' + str(time()))
        with ah_db.open_db_connection('datascience') as connection:
            result = connection.execute(sql)
            row = result.fetchone()
        return row['cntExp']

    def getNestedExperiment(self, eid):
        sql = """
              SELECT NestedExperimentId, GroupIdInNestedExperiment,
              DefaultGroupIdInNestedExperiment
              FROM DataScience.Experiment
              WHERE experimentid={}
              """.format(eid)

        #print('getNestedExperiment '+str(time()))
        with ah_db.open_db_connection('datascience') as connection:
            result = connection.execute(sql)
            row = result.fetchone()

        return row['NestedExperimentId'], row['GroupIdInNestedExperiment'], \
               row['DefaultGroupIdInNestedExperiment']

    def getUserGroup(self, uid, eid):
        #print('getUserGroup ' + str(time()))
        expExist = self.getExperimentDateRange(eid)
        if not expExist:
            return 1

        nestedExpId, nestedGroupId, defnestedGroupId = \
            self.getNestedExperiment(eid)

        if all([nestedExpId, nestedGroupId, defnestedGroupId]):
            ngid = self.getUserGroup(uid, nestedExpId)
            if ngid == nestedGroupId:
                return self.pullCreateUserGroup(uid, eid)
            else:
                return defnestedGroupId
        else:
            return self.pullCreateUserGroup(uid, eid)

    def pullCreateUserGroup(self, uid, eid):
        sql = '''
              SELECT GroupId
              FROM DataScience.ExperimentGroup
              WHERE userid={0} and experimentid={1}
              '''.format(uid, eid)

        #print('pullCreateUserGroup ' + str(time()))
        with ah_db.open_db_connection('datascience') as connection:
            result = connection.execute(sql)
            row = result.fetchone()
        #     print( row)
        if row is not None:
            return row['GroupId']
        else:
            return self.assignUserGroup(eid, uid)

    def getUserGroupForUXExp(self, eids, uid):
        #print('getUserGroupForUXExp '+str(time()))
        if len(eids) == 0:
            return []
        else:
            eidsStr = "(" + ",".join([str(x) for x in eids]) + ")"
            sql = '''
                  SELECT eg.ExperimentId, eg.GroupId, egd.GroupDescription
                  FROM DataScience.ExperimentGroup eg
                  JOIN DataScience.ExperimentGroupDescriptions egd
                  ON eg.ExperimentId = egd.ExperimentId AND eg.GroupId = egd.GroupId
                  WHERE eg.userid=%d and eg.experimentid IN %s
                  ''' % (uid, eidsStr)

            with ah_db.open_db_connection('datascience') as connection:
                result = connection.execute(sql)
                rows = result.fetchall()

            var = self.getExperimentVariablesForUxExp(eids)

            [row.update({'Variables':
                         var[row['ExperimentId']][row['GroupId']]}) for row in rows]
            return rows

    def getGroupUsers(self, eid, gid):
        #print('getGroupUsers '+str(time()))
        sql = '''
              SELECT UserId
              FROM DataScience.ExperimentGroup
              WHERE ExperimentId = {0} AND GroupId = {1}
              '''.format(eid, gid)

        with ah_db.open_db_connection('datascience') as connection:
            result = connection.execute(sql)
            rows = result.fetchall()

        uids = [x[0] for x in rows]
        return uids

    def assignUserGroup(self, eid, uid):
        #print('assignUserGroup '+str(time()))
        splits = self.getGroupSplit(eid)
        g = int(np.random.choice(range(1, len(splits)+1), p=splits))

        sql = '''
              INSERT INTO DataScience.ExperimentGroup
              (ExperimentId, UserId, GroupId, CreatedOn)
              VALUES ({0}, {1}, {2}, CURRENT_TIMESTAMP)
              '''.format(eid, uid, g)

        with ah_db.open_db_connection('datascience') as connection:
            connection.execute(sql)
        return g

    def mandatoryAssignUserGroup(self, eid, uid, groupid):
        sql = '''
              INSERT INTO DataScience.ExperimentGroup
              (ExperimentId, UserId, GroupId, CreatedOn)
              VALUES ({0}, {1}, {2}, CURRENT_TIMESTAMP)
              '''.format(eid, uid, groupid)

        with ah_db.open_db_connection('datascience') as connection:
            connection.execute(sql)

    def mandatoryReassignUserGroup(self, eid, uid, groupid):
        sql = '''
              UPDATE DataScience.ExperimentGroup
              SET GroupId = {0}
              WHERE experimentid = {1} AND UserId = {2}
              '''.format(groupid, eid, uid)

        with ah_db.open_db_connection('datascience') as connection:
            connection.execute(sql)

    def assignUserGroupForUXExp(self, eids, uid):
        #print('assignUserGroupForUXExp '+str(time()))
        gids = []
        ds = []
        for eid in eids:
            try:
                g = self.assignUserGroup(eid, uid)
            except:
                g = self.getUserGroup(uid, eid)
                log = getLogger('ah.ExperimentGroup')
                log.warning('User experiment group assignment duplicated'
                            '(experimentId = %s)'
                            '(userid = %s)' % (eid, uid))
            gids.append(g)
            desc = self.getExpGroupDescriptions(eid)
            ds.append(desc[g])
        var = self.getExperimentVariablesForUxExp(eids)
        return [{'ExperimentId': x[0],
                 'GroupId': x[1],
                 'GroupDescription': x[2],
                 'Variables': var[x[0]][x[1]]} for x in zip(eids, gids, ds)]

    def getExpGroupDescriptions(self, eid):
        #print('getExpGroupDescriptions '+str(time()))
        sql = '''
              SELECT GroupId, GroupDescription
              FROM DataScience.ExperimentGroupDescriptions
              WHERE experimentid={0}
              ORDER BY groupid
              '''.format(eid)

        with ah_db.open_db_connection('datascience') as connection:
            result = connection.execute(sql)
            rows = result.fetchall()
        # group id has to be continuous and starting from 1
        return {x['GroupId']: x['GroupDescription'] for x in rows}

    def getGroupSplit(self, eid):
        #print('getGroupSplit '+str(time()))
        sql = '''
               SELECT split
               FROM DataScience.ExperimentGroupDescriptions
               WHERE experimentid={0}
               ORDER BY groupid
               '''.format(eid)

        # group id has to be continuous and starting from 1
        with ah_db.open_db_connection('datascience') as connection:
            result = connection.execute(sql)
            rows = result.fetchall()

        if len(rows) == 0:
            raise NameError('No Group Split for experiment: {}'.format(eid))
        else:
            return [row['split'] for row in rows]

    def getExperimentVariables(self, eid):
        #print('getExperimentVariables '+str(time()))
        sql = '''
              SELECT GroupId, VariableName, VariableValue
              FROM DataScience.UXExperimentVariables
              WHERE experimentid={0}
              '''.format(eid)

        # group id has to be continuous and starting from 1
        with ah_db.open_db_connection('datascience') as connection:
            result = connection.execute(sql)
            rows = result.fetchall()

        GroupIds = set([x['GroupId'] for x in rows])
        return {y: {x['VariableName']: x['VariableValue']
                    for x in rows if x['GroupId'] == y} for y in GroupIds}

    def getExperimentVariablesForUxExp(self, eids):
        #print('getExperimentVariablesForUxExp '+str(time()))
        varDict = {}
        for eid in eids:
            ExpVars = self.getExperimentVariables(eid)
            varDict[eid] = ExpVars
        return varDict

    def getAllUserId(self):
        #   return [6355,100305,45563]
        #print('getAllUserId '+str(time()))
        sql = '''
                SELECT distinct f.userid
                from moneymovement.transactions t
                JOIN moneymovement.transactionstofundstransfers ttft
                ON t.TransactionID = ttft.TransactionID
                JOIN moneymovement.FundsTransfers f
                ON ttft.FundsTransferId = f.FundsTransferId
                where  t.TransactionTypeID=3 and t.StatusID in (2,3)
                and f.PostingDate>'2016-01-01'
                order by f.userid
                '''

        try:
            with ah_db.open_db_connection('moneyMovement') as connection:
                result = connection.execute(sql)
                r = result.fetchall()
        except:
            print(traceback.format_exc())
        return [l[0] for l in r]

    def getNewUserId(self):
        #   return [6355,100305,45563]
        #print('getNewUserId '+str(time()))
        sql = '''
                SELECT DISTINCT UserId
                FROM miscellaneous.Users
                WHERE CreatedOn >= '2017-02-15'
                '''
        try:
            with ah_db.open_db_connection('miscellaneous') as connection:
                result = connection.execute(sql)
                r = result.fetchall()
        except:
            print(traceback.format_exc())
        return [l[0] for l in r]

    def assignAllUserGroup(self, eid):
        #print('assignAllUserGroup '+str(time()))
        uids = self.getAllUserId()
        for uid in uids:
            self.assignUserGroup(eid, uid)

    def isExperimentValid(self, eid):
        #print('isExperimentValid '+str(time()))
        sql = '''
              SELECT (CASE WHEN CURDATE() BETWEEN
                     StartTime AND EndTime THEN 1 ELSE 0 END) AS valid
              FROM DataScience.Experiment
              WHERE ExperimentId = {0}
              '''.format(eid)

        with ah_db.open_db_connection('datascience') as connection:
            result = connection.execute(sql)
            row = result.fetchone()

        #     print( row)
        if len(row) == 0:
            raise NameError('request of experiment group'
                            'assignment failure,'
                            'experiment does not exist'
                            ', experimentid = %s.' % eid)
        else:
            return row['valid']


class TransactionExperiment:
    def __init__(self,conn=None):
        self.conn=conn

    def getUserGroup(self,uid,eid):
        splits=self.getGroupSplit(eid)
        rollingsum=0
        r=random.random()
      #  print(r)
        g=1
     #   print(r,splits)
        for s in splits:
            rollingsum+=s
            if r<rollingsum: break
            g+=1

        return g

    def getGroupSplit(self,eid):
        sql = '''
                 select split 
                 from DataScience.ExperimentGroupDescriptions
                 where experimentid={0}
                 order by groupid
                 '''.format(eid)

        #group id has to be continuous and starting from 1
        with ah_db.open_db_connection('datascience') as connection:
            result = connection.execute(sql)
            rows = result.fetchall()
        return [row['split'] for row in rows]

def initExperiment(eid):

    myExp=UserExperiment()
    myExp.assignAllUserGroup(eid)



def getUserIds(conn):
    sql = """
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
        select distinct UserId
        from dbo.Activations
        Where HourStatusID not in (8,9,10)
    """
    df = getSQLdata(sql, conn)
    return df


if __name__ == "__main__":

    import ah_db
    import ah_config
    ah_config.initialize()

    """
    with ah_db.open_db_connection('sqlserver') as connection:
        myExp = UserExperiment(conn)

        uid = 547078

        # a = myExp.getUserGroupForUXExp([9], 547078)
        b = myExp.getExperimentVariablesForUxExp([9, 10])
        a = myExp.getUserGroupForUXExp([9, 10], uid)
        # eids = myExp.getAllCurrentUXExperiment()

        # # print(myExp.assignUserGroupForUXExp(eids, 180198))
        # print(myExp.getUserGroupForUXExp(eids, 180198))

        # print(myExp.assignUserGroupForUXExp([99], 3))
    """

    eid = 20
    UserId = 420
    exp = UserExperiment()
    print('All current UX experiment')
    print(exp.getAllCurrentUXExperiment())

    print('Experiment date range')
    print(exp.getExperimentDateRange(eid))

    print('Nested experiment')
    print(exp.getNestedExperiment(eid))

    print('User group')
    print(exp.getUserGroup(UserId, eid))
    print(exp.getUserGroup(UserId, 88))

    #print('User group for UX experiment')
    #print(exp.getUserGroupForUXExp([eid], UserId))

    #print('Group Users')
    #print(exp.getGroupUsers(eid, 1))

    #print('Assignment')
    #exp.assignUserGroup(eid, UserId)

    print('Group description')
    print(exp.getExpGroupDescriptions(eid))

    print('Group Split')
    print(exp.getGroupSplit(eid))

    print('Experiment Variables')
    print(exp.getExperimentVariables(eid))

    print('Experiment is valid')
    print(exp.isExperimentValid(eid))



    # df = getUserIds(conn)
    # eid = 20
    # UserId = 420
    # myExp = UserExperiment()
    # group = myExp.getUserGroup(UserId, eid)
    # print(group)
