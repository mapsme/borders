import pandas as pd
import numpy as np

from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.model_selection import (
    cross_val_score,
    KFold,
    GridSearchCV,
)
from sklearn.svm import SVR
from sklearn.preprocessing import StandardScaler


data = pd.read_excel('mwm_data.xlsx', sheet_name='mwms_all', header=1)
data = data[data['exclude'] == 0]
#data['is_urban2'] = data.apply(lambda row: row['pop_density'] > 260, axis=1) # 260 - median of pop_density

popul_column = 'urban_pop'  # options are 'population and 'urban_pop' (for population of cities and towns only)
feature_names = [popul_column, 'area', 'city_cnt', 'hamlet_cnt']
target_name = 'size'

for feature in set(feature_names) - set(['area']):  # if area is None it's an error!
    data[feature] = data[feature].fillna(0)


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

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=0)

        C_array = [10 ** n for n in range(6, 7)]
        gamma_array =  [0.009 + i * 0.001 for i in range(-7, 11, 2)] + ['auto', 'scale']
        epsilon_array = [0.5 * i for i in range(0, 15)]
        coef0_array = [-0.1, -0.01, 0, 0.01, 0.1]
        param_grid = [
            {'kernel': ['linear'], 'C': C_array, 'epsilon': epsilon_array},
            {'kernel': ['rbf'], 'C': C_array, 'gamma': gamma_array, 'epsilon': epsilon_array},
            {'kernel': ['poly', 'sigmoid'],
                    'C': C_array, 'gamma': gamma_array, 'epsilon': epsilon_array, 'coef0': coef0_array},
        ]

        svr = SVR()
        grid_search = GridSearchCV(svr, param_grid, scoring=scoring)
        grid_search.fit(X_train, y_train)
        #means = grid_search.cv_results_['mean_test_score']
        #stds = grid_search.cv_results_['std_test_score']
        #print("Grid scores on development set:")
        #for mean, std, params in zip(means, stds, grid_search.cv_results_['params']):
        #    print("%0.3f (+/-%0.03f) for %r" % (mean, std, params))

        print("C", C_array)
        print("gamma", gamma_array)
        print("epsilon", epsilon_array)
        print("coef0", coef0_array)
        print("Best_params:", grid_search.best_params_, grid_search.best_score_)


def train_and_serialize_model(sample):
    X = sample[feature_names]
    y = sample[target_name]

    X_head = X[0:4]
    scaler = StandardScaler()
    X = scaler.fit_transform(X)

    # Parameters tuned with GridSearch
    regressor = SVR(kernel='rbf', C=10**6, epsilon=0.0, gamma=0.012)
    regressor.fit(X, y)

    print(regressor.predict(X[0:4]))

    # Serialize model
    import pickle
    with open('model.pkl', 'wb') as f:
        pickle.dump(regressor, f)
    with open('scaler.pkl', 'wb') as f:
        pickle.dump(scaler, f)

    # Deserialize model and test it on X_head samples
    with open('model.pkl', 'rb') as f:
        regressor2 = pickle.load(f)
        with open('scaler.pkl', 'rb') as f:
            scaler2 = pickle.load(f)
        print(regressor2.predict(scaler2.transform(X_head)))


if __name__ == '__main__':
    train_and_serialize_model(data)

