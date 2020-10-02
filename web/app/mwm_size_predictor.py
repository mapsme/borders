import numpy as np
import pickle

import config


class MwmSizePredictor:

    def __init__(self):
        with open(config.MWM_SIZE_PREDICTION_MODEL_PATH, 'rb') as f:
            self.model = pickle.load(f)
        with open(config.MWM_SIZE_PREDICTION_MODEL_SCALER_PATH, 'rb') as f:
            self.scaler = pickle.load(f)

    def predict(self, features_array):
        """1D or 2D array of feature values for predictions. Features are
        'urban_pop', 'area', 'city_cnt', 'hamlet_cnt' as defined for the
        prediction model.
        """
        X = np.array(features_array)
        one_prediction = (X.ndim == 1)
        if one_prediction:
            X = X.reshape(1, -1)
        X_scaled = self.scaler.transform(X)
        predictions = self.model.predict(X_scaled)
        if one_prediction:
            return predictions[0]
        else:
            return predictions.tolist()
