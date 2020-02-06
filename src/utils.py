from flask import make_response
import json


class ResourceException(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def __str__(self, *args, **kwargs):
        return "%s (%d)" % (self.message, self.status_code)

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv


def output_json(data, code, headers=None):
    content_type = 'application/json'
    dumped = json.dumps(data)
    if headers:
        headers.update({'Content-Type': content_type})
    else:
        headers = {'Content-Type': content_type}
    response = make_response(dumped, code, headers)
    return response


def getWeekNumber(inputdate):
 #   return inputdate.isocalendar()[1]
    startingDate=date(2012,12,30)
    return (inputdate-startingDate).days/7


def string2date(datestring):
    return datetime.strptime(datestring[:10], "%Y-%m-%d").date()


def string2time(datestring):
    return datetime.strptime(datestring[:19], "%Y-%m-%d %H:%M:%S")


def calcRate(a,b):
    if b<=0:
       # print "rate calculation Warning:",a,b;
        return 0
    else: return a*1.0/b


def printArray(myarray,fout):
    for x in myarray:  print >>fout, x,


def stringMatch(l,s):
    #chec if any of l(list) in s : case insenstive
    for i in l:
        i=i.lower()
        if i in s: return True
    return False


def dedupe(items, key=None):
    seen = set()
    for item in items:
        val = item if key is None else key(item)
        if val not in seen:
            yield item
            seen.add(val)


def compare_data_frames(df1, df2):

    #   Normalize the column names in the data frames
    df1.columns = [c.lower() for c in df1.columns]
    df2.columns = [c.lower() for c in df2.columns]

    #   See if the number of columns is the same
    columns1 = sorted(df1)
    columns2 = sorted(df2)
    assert len(columns1) == len(columns2), "Columns are not the same:\n%s\n%s" % (columns1, columns2)

    #   Check the types for each column
    for col_name in columns1:
        t1 = df1[col_name].dtype
        t2 = df2[col_name].dtype
        assert t1 == t2, "Column types are different '%s'\n%s\n%s" % (col_name, t1, t2)

    diffs = []
    for index, row in df1.iterrows():
        print("Comparing data for index", index)
        row1 = df1.loc[index]
        row2 = df2.loc[index]
        for col_name in columns1:
            t1 = df1[col_name].dtype
            v1 = row1[col_name]
            v2 = row2[col_name]

            # print(col_name, "=>", t1, type(v1))
            # if not (np.isnat(v1) and np.isnat(v2)):
            if ('datetime64[ns]' == t1):
                #   Account for 'NaT'
                v1 = str(v1)
                v2 = str(v2)
            elif ('float64' == t1):
                #   Account for 'nan'
                v1 = str(v1)
                v2 = str(v2)

            if not (v1 == v2):
                diffs.append("Values are different for index %s['%s'](%s):\n%s(%s)\n%s(%s)" % (index, col_name, t1, v1, type(v1), v2, type(v2)))
            # assert v1 == v2,

    if (len(diffs) > 0):
        print("\n***** Differences *****")
        for diff in diffs:
            print(diff)


def getEmailLocal(email):

    local,domain=splitEmail(email)
    return local 

def splitEmail(email):
    email=email.lower().split('@')
    local=email[0].strip()
    if len(email)>1: domain=email[1]
    else: domain=''
    
    if domain=='gmail.com':
        local=local.split('+')[0].replace('.','')
    return local,domain


def longestCommonSubsequence(A, B):
        if not A or not B:
            return 0

        lenA, lenB = len(A), len(B)
        lcs = [[0 for i in range(1 + lenB)] for j in range(1 + lenA)]
        
        for i in range(1, 1 + lenA):
            for j in range(1, 1 + lenB):
                if A[i - 1] == B[j - 1]:
                    lcs[i][j] = 1 + lcs[i - 1][j - 1]
                else:

                    lcs[i][j] = max(lcs[i - 1][j], lcs[i][j - 1])
        
        return lcs[lenA][lenB]
        
        

def longestCommonSubstr(S,T):
    m = len(S)
    n = len(T)
    counter = [[0]*(n+1) for x in range(m+1)]
    longest = 0
   # lcs_set = set()
    for i in range(m):
        for j in range(n):
            if S[i] == T[j]:
                c = counter[i][j] + 1
                counter[i+1][j+1] = c
                if c > longest:
           #         lcs_set = set()
                    longest = c
               #     lcs_set.add(S[i-c+1:i+1])
#                elif c == longest:
 #                   lcs_set.add(S[i-c+1:i+1])

    return longest
    