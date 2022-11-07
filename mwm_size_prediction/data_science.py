import pandas as pd
import numpy as np
import math

from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.model_selection import (
    cross_val_score,
    KFold,
    GridSearchCV,
)
from sklearn.svm import SVR
from sklearn.preprocessing import StandardScaler


#def fit_mwm_size(df):
#    df['mwm_size'] = np.where(df['mwm_size'].isnull(), df['mwm_size_sum'], df['mwm_size'])


#data1 = pd.read_csv('data/4countries.csv', sep=';')  # Austria, Belgium, Netherlands, Germany
#data2 = pd.read_csv('data/7countries.csv', sep=';')  # Norway, UK, US(4 states), Switzerland, Japan, Belarus, Ile-de-France
data = pd.read_csv('data/countries.csv', sep=';')

#data = pd.concat([data1, data2])
data = data[data.excluded.eq(0) & data.id.notnull()]
#fit_mwm_size(data)


popul_column = 'city_pop'  # options are 'population and 'city_pop' (for population of cities and towns only)
feature_names = [popul_column, 'land_area', 'city_cnt', 'hamlet_cnt', 'coastline_length']
target_name = 'size'

for feature in set(feature_names) - set(['land_area']):  # if area is None it's an error!
    data[feature] = data[feature].fillna(0)



def check_summing(data):
    for index, row in data.iterrows():
        for column in feature_names + [target_name,]:
            r_id = row['id']
            children = data[data['parent_id'] == r_id]
            if children.empty:
                continue
            sum = children[column].sum()
            value = row[column]
            if not math.isclose(sum, value):
                print(f"Different {column} for {row['region_name']}: {sum} != {value}")
                return


scoring = 'neg_mean_squared_error'  # another option is 'r2'


def my_cross_validation(sample):
    X = sample[feature_names]
    y = sample[target_name]

    sc_X = StandardScaler()
    X = sc_X.fit_transform(X)

    lin_regression = LinearRegression(fit_intercept=False)
    svr_linear = SVR(kernel='linear')
    svr_rbf = SVR(kernel='rbf')

    for estimator_name, estimator in zip(
            ('LinRegression', 'SVR_linear', 'SVR_rbf'),
            (lin_regression, svr_linear, svr_rbf)):
        cv_scores = cross_val_score(estimator, X, y,
                                 cv=KFold(5, shuffle=True, random_state=1),
                                 scoring=scoring)
        mean_score = np.mean(cv_scores)
        print(f"{estimator_name:15}", cv_scores, mean_score)


def my_grid_search(sample):
        X = sample[feature_names]
        y = sample[target_name]

        sc_X = StandardScaler()
        X = sc_X.fit_transform(X)

        C_array = [10 ** n for n in range(5, 6)]
        #gamma_array =  [0.0001 + i * 0.00001 for i in range(-4, 5)] + ['auto', 'scale']
        #epsilon_array = [0.02 + i*0.002 for i in range(-4, 5)]
        gamma_array = [0, 100, 10000] + ['auto', 'scale']
        epsilon_array = [0, 1, 10]
        #coef0_array = [0, 0.01, 0.1, 1, 10]
        param_grid = [
           # {'kernel': ['linear'], 'C': C_array, 'epsilon': epsilon_array},
            {'kernel': ['rbf'], 'C': C_array, 'gamma': gamma_array, 'epsilon': epsilon_array},
           # {'kernel': ['poly'], #'sigmoid'],
           #         'C': C_array, 'gamma': gamma_array, 'epsilon': epsilon_array, 'coef0': coef0_array},
        ]

        svr = SVR()
        grid_search = GridSearchCV(svr, param_grid, scoring=scoring)
        grid_search.fit(X, y)
        #means = grid_search.cv_results_['mean_test_score']
        #stds = grid_search.cv_results_['std_test_score']
        #print("Grid scores on development set:")
        #for mean, std, params in zip(means, stds, grid_search.cv_results_['params']):
        #    print("%0.3f (+/-%0.03f) for %r" % (mean, std, params))

        print("C", C_array)
        print("gamma", gamma_array)
        print("epsilon", epsilon_array)
        #print("coef0", coef0_array)
        print("Best_params:", grid_search.best_params_, grid_search.best_score_)
        return
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=0)
        svr = SVR(**grid_search.best_params_)
        svr.fit(X_train, y_train)
        test_pred = svr.predict(X_test)
        print(test_pred[:10])
        print(y_test[:10].to_numpy())


def train_and_serialize_model(sample):
    #sample = sample[~sample.id.isin(test_ids)]
    X = sample[feature_names]
    y = sample[target_name]

    X_head = X[0:4]
    scaler = StandardScaler()
    X = scaler.fit_transform(X)

    #regressor = SVR(kernel='rbf', C=10**6, epsilon=0.0, gamma=0.012) # Init
    regressor = SVR(kernel='rbf', C=10 ** 7, epsilon=0.2, gamma=5e-5)
    regressor.fit(X, y)

    print(regressor.predict(X[0:4]))

    # Serialize model
    import pickle
    with open('model_ex_not_full.pkl', 'wb') as f:
        pickle.dump(regressor, f)
    with open('scaler_ex_not_full.pkl', 'wb') as f:
        pickle.dump(scaler, f)

    # Deserialize model and test it on X_head samples
    with open('model_ex_not_full.pkl', 'rb') as f:
        regressor2 = pickle.load(f)
        with open('scaler_ex_not_full.pkl', 'rb') as f:
            scaler2 = pickle.load(f)
        print(regressor2.predict(scaler2.transform(X_head)))


def test_predictions(data):

    test_sample = data[data.id.isin(test_ids)]
    X = test_sample[feature_names]
    y = test_sample[target_name]

    print(y.to_list())
    # Deserialize model and test it on X_head samples
    import pickle
    with open('model_ex_not_full.pkl', 'rb') as f:
        regressor = pickle.load(f)
        with open('scaler_ex_not_full.pkl', 'rb') as f:
            scaler = pickle.load(f)
    predictions = regressor.predict(scaler.transform(X))
    predictions = [round(x, 1) for x in predictions]
    print(predictions)

"""
def train_model_at_all_and_test_on_leaves(sample):
X = sample[feature_names]
y = sample[target_name]

scaler = StandardScaler()
X = scaler.fit_transform(X)

regressor = SVR(kernel='rbf', C=10 ** 5, epsilon=5, gamma='auto')
regressor.fit(X, y)

predictions = regressor.predict(X)
diff = y - predictions
mse = np.sqrt((diff*diff).sum()/len(diff))
#47724.23021774437
me = abs(diff).sum()/len(diff)
#3958.586729638536

leaves = sample[sample['is_leaf'] == 1]
X_leaves = leaves[feature_names]
y_leaves = leaves[target_name]

l_predictions = regressor.predict(scaler.transform(X_leaves))
l_diff = y_leaves - l_predictions
l_mse = np.sqrt((l_diff*l_diff).sum()/len(l_diff))
#3589.8199502064326
l_me = abs(l_diff).sum()/len(l_diff)
#1475.6617896408952
"""


if __name__ == '__main__':
    my_grid_search(data)
    #train_and_serialize_model(data)
    #test_predictions(data)

