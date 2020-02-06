import pandas as pd
import numpy as np
#import matplotlib.pyplot as plt
#
# def oos_split(data, train_size):
#     return train_test_split(data, train_size=train_size)


def woeTransformation(X_Train_in, y_Train, bins=20):
    X_Train = X_Train_in.copy()
    woe_dict = dict()
    for feature in X_Train.columns:
        X_series = X_Train[feature]
        if isContinuous(X_series):
            print(feature)
            woe, bounds, iv = Transform_WOE_continuous_balanced_target(
                X_series.values, y_Train.values, bins=bins)
            woe_dict[feature] = (woe, bounds, iv)
            X_Train.loc[:, feature] = X_Train.loc[:, feature].\
                map(lambda x: mapToWOE(x, woe, bounds))

            print(iv)
            plotWOE(X_Train[feature].values, y_Train.values, feature, iv)

    return X_Train, woe_dict


def woeTransformation_predict(X_predict, woe_dict):
    for feature in woe_dict:
        woe, bounds, _ = woe_dict[feature]
        X_predict.loc[:, feature] = X_predict.loc[:, feature].\
            map(lambda x: mapToWOE(x, woe, bounds))
    return X_predict


def Transform_WOE_continuous_balanced_target(feature_array, label, bins=20):

    oneTotal = np.sum(label)
    zeroTotal = len(label) - oneTotal

    if feature_array.shape[0] != label.shape[0]:
        raise TypeError('Feature and label do not have the same size!')
    N = feature_array.shape[0]
    binSize = int(N/(bins-1))
    label = [y for (x, y) in sorted(zip(feature_array, label))]
    feature_array = sorted(feature_array)

    woe = list()
    bounds = list()
    iv = 0

    index = 0
    bounds.append(-np.inf)
    index_old = index
    index += binSize-1

    while index < N-binSize:
        index = np.where(feature_array == feature_array[index])[0][-1]
        bounds.append(feature_array[index])
        woe_tmp, _ = calculateWOE(label[index_old:index+1],
                                  oneTotal, zeroTotal)
        woe.append(woe_tmp)
        iv += _
        index_old
        index += binSize-1

    bounds.append(np.inf)
    woe_tmp, _ = calculateWOE(label[index_old:], oneTotal, zeroTotal)
    woe.append(woe_tmp)
    iv += _

    return woe, bounds, iv


def Transform_WOE_continuous_low_bad_rate(feature_array, label, bins=20):

    oneTotal = np.sum(label)
    zeroTotal = len(label) - oneTotal

    if feature_array.shape[0] != label.shape[0]:
        raise TypeError('Feature and label do not have the same size!')
    binSize = int(oneTotal/(bins-1))
    label = [y for (x, y) in sorted(zip(feature_array, label))]
    feature_array = sorted(feature_array)
    label_cumsum = np.cumsum(label)

    woe = list([0]*bins)
    bounds = list([0]*(bins+1))
    iv = 0

    i = 0
    bounds[i] = -np.inf
    index = np.where(label_cumsum == binSize*(i+1))[0][-1]
    woe[i], _ = calculateWOE(label[:index+1], oneTotal, zeroTotal)
    iv += _

    for i in range(1, bins-1):
        bounds[i] = feature_array[index]
        index_old = index
        index = np.where(label_cumsum == binSize*(i+1))[0][-1]
        woe[i], _ = calculateWOE(label[index_old:index+1],
                                 oneTotal, zeroTotal)
        iv += _

    i += 1
    bounds[i] = feature_array[index]
    bounds[i+1] = np.inf
    woe[i], _ = calculateWOE(label[index:], oneTotal, zeroTotal)
    iv += _

    return woe, bounds, iv


def mapToWOE(x, woe, bounds):
    assert len(woe) == len(bounds) - 1, "different numbers of bins and WOEs"
    bins = len(woe)
    i = 0
    while i < bins:
        if x <= bounds[i+1]:
            break
        else:
            i += 1
    return woe[i]


def isContinuous(feature_series):
    unq_size = feature_series.unique().shape[0]
    if unq_size > 3:
        return 1
    else:
        return 0


def calculateWOE(y, oneTotal, zeroTotal):
    myeps = 1e-6
    oneRate = oneTotal/(oneTotal + zeroTotal)
    zeroRate = zeroTotal/(oneTotal + zeroTotal)
    alpha = 100
    n_one = np.sum(y)
    n_zero = len(y) - n_one
    oneRatio = n_one/oneTotal
    zeroRatio = n_zero/zeroTotal
#    print(oneRatio, zeroRatio)
    woe = np.log(n_one /
                 n_zero)

    iv = (oneRatio - zeroRatio)*woe

    return woe, iv


def oot_split(data, split_time):
    data_Train = data[(data.predTime.astype('datetime64') <= split_time)]
    data_Test = data[(data.predTime.astype('datetime64') > split_time)]
    return data_Train, data_Test


def oou_split(data, train_size=0.7, seed=None):
    np.random.seed(seed)
    users = data.uid.unique()
    Nuser = users.shape[0]
    user_train = np.random.\
        choice(users, round(train_size * Nuser), replace=False)

    data_Train = data[(data['uid'].map(lambda x: x in user_train))]
    data_Test = data[(data['uid'].map(lambda x: x not in user_train))]
    return data_Train, data_Test


def convertCatToDummy(df_in, catNames, cutoff=0):
    df = df_in.copy()
    df_cat = df[catNames].fillna(65535)
    df_other = df.drop(catNames, 1)
    dummy_rules = dict()

    for cat in catNames:
        if df_cat[cat].dtype == float:
            df_cat.loc[:, [cat]] = df_cat[cat].astype('int').replace(65535, 'NaN')
        if df_cat[cat].dtype == int:
            df_cat.loc[:, [cat]] = df_cat[cat].astype('object')
        # if df_cat[cat].dtype == object:
        #     df_cat.loc[:, [cat]] = df_cat[cat].astype('object')

        counter = df_cat[cat].value_counts().to_dict()
        dummy_rules[cat] = [str(key) for key, count in counter.items()
                            if count >= cutoff]
        if any([i < cutoff for i in counter.values()]):
            df_cat.loc[(df_cat[cat].map(counter) < cutoff), [cat]] = 'other'
            dummy_rules[cat].append('other')

    df_cat = pd.get_dummies(df_cat, catNames)

    return pd.concat((df_other, df_cat), axis=1), dummy_rules


def dummyCoding(df_in, catNames, dummy_rules):
    df = df_in.copy()
    df_cat = df[catNames].fillna(65535)
    df_other = df.drop(catNames, axis=1)

    col_names = []
    for cat in catNames:
        for value in dummy_rules[cat]:
            col_names.append(cat+'_'+str(value))
    P = len(col_names)
    df_out = pd.DataFrame(np.zeros((1, P)), columns=col_names, index=df.index)
    for cat in catNames:
        if df_cat[cat].dtype == float:
            if '65535' in dummy_rules[cat]:
                df_cat.loc[:, [cat]] = df_cat[cat].astype('int')
            else:
                df_cat.loc[:, [cat]] = df_cat[cat].astype('int').replace(65535, 'NaN')
        # if df_cat[cat].dtype == int:
        #     df_cat.loc[:, [cat]] = df_cat[cat].astype('object')

        if str(df_cat[cat].values[0]) in dummy_rules[cat]:
            col = df_cat[cat].values[0]
            df_out[cat+'_'+str(col)] = 1
        elif 'other' in dummy_rules[cat]:
            df_out[cat+'_other'] = 1
    return pd.concat((df_other, df_out), axis=1)


def dataNormalization_Train(df_in, centered=True):
    df = df_in.astype('float')
    mean = df.mean()
    mean.name = 'center'
    lowerbound = df.quantile(0.05)
    lowerbound.name = 'lowerbound'
    upperbound = df.quantile(0.95)
    upperbound.name = 'upperbound'
    if centered:
        return ((df - mean)/(upperbound-lowerbound).map(lambda x: [x, 1][x == 0])).\
        applymap(lambda x: max(min(x, 1), -1)), \
        pd.concat([mean, upperbound, lowerbound], axis=1)
    else:
        return ((df - lowerbound)/(upperbound-lowerbound).map(lambda x: [x, 1][x == 0])).\
        applymap(lambda x: max(min(x, 1), 0)), \
        pd.concat([upperbound, lowerbound], axis=1)


def dataNormalization_Predict(df_in, nl):
    df = df_in.astype('float')
    if nl.shape[1] == 3:
        return ((df - nl.center)/(nl.upperbound-nl.lowerbound).map(lambda x: [x, 1][x == 0])).\
        applymap(lambda x: max(min(x, 1), -1))
    else:
        return ((df - nl.lowerbound)/(nl.upperbound-nl.lowerbound).map(lambda x: [x, 1][x == 0])).\
        applymap(lambda x: max(min(x, 1), 0))


def plotWOE(X, y, feature, iv):
    XtoX_n = dict()
    for i in range(len(X)):
        if X[i] in XtoX_n:
            XtoX_n[X[i]][0] += 1
            XtoX_n[X[i]][1] += y[i]
        else:
            XtoX_n[X[i]] = [1, y[i]]

    newX = {x: y[1]/y[0] for x, y in XtoX_n.items()}
    x = list(newX.keys())
    y = list(newX.values())
    plt.figure()
    plt.plot(x, y, 'o')
    plt.title(feature+' iv='+str(iv))
    plt.savefig('./woe/'+feature+'.png', dpi=150)
    plt.close()


if __name__ == "__main__":
    import numpy as np
    import matplotlib.pyplot as plt
    X = np.random.rand(10000)
    y = (np.random.rand(10000) > 0.5).astype(int)
    woe, bounds, iv = Transform_WOE_continuous_balanced_target(X, y)
    # woe, bounds, iv = Transform_WOE_continuous_low_bad_rate(X, y)
    X_n = list(map(lambda x: mapToWOE(x, woe, bounds), X))

    XtoX_n = dict()
    for i in range(len(X)):
        if X_n[i] in XtoX_n:
            XtoX_n[X_n[i]][0] += 1
            XtoX_n[X_n[i]][1] += y[i]
        else:
            XtoX_n[X_n[i]] = [1, y[i]]

    newX = {x: y[1]/y[0] for x, y in XtoX_n.items()}
    xx = list(newX.keys())
    yy = list(newX.values())
    plt.plot(xx, yy, 'o')
    plt.show()
