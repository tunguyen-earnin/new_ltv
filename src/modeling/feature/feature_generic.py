class FeatureGeneric(object):
    nWkMax = 20

    def __init__(self, uid):
        self.uid = uid
        self.data = self.getData()

    def getDataRangeIndex(self, predTime):
        r = -1
        for i in range(len(self.data)-1, -1, -1):
            if self.data[i]['CreatedOn'] is not None and \
               predTime > self.data[i]['CreatedOn']:
                r = i
                break
        return r

    def buildFeatures(self, predTime):
        f = {}
        self.reName(f, 'cat_')
        return f

    def reName(self, f, cat):
        for k in list(f.keys()):
            f[cat+k] = f.pop(k)

    def getData(self):
        return []
