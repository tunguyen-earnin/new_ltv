from modeling.model.risk_model.max_adjustment_model.\
    MaxAdjustmentModel import Predictor
import pandas as pd
import traceback
import csv

if __name__ == '__main__':

    df = pd.read_csv('modeling/model/risk_model/max_adjustment_model/data/master.csv', low_memory=False)

    N = df.shape[0]

    f = open('activeuser_max_reason_dist.csv', 'w')

    first = 1

    for i in range(10):
        u = df.iloc[i]
        predictor = Predictor(u.to_dict())
        try:
            print(i)
            reasons = predictor.getReasonCategory()
            reasons['UserId'] = u['uid']
            if first == 1:
                writer = csv.DictWriter(f, fieldnames=list(reasons.keys()))
                writer.writeheader()
                first = 0
                writer.writerow(reasons)
            else:
                writer.writerow(reasons)
        except:
            traceback.print_exc()

    f.close()
