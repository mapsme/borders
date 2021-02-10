import numpy as np
import pickle

import config


class MwmSizePredictor:

    factors = ('city_pop', 'land_area', 'city_cnt', 'hamlet_cnt',
               'coastline_length')

    def __init__(self):
        with open(config.MWM_SIZE_PREDICTION_MODEL_PATH, 'rb') as f:
            self.model = pickle.load(f)
        with open(config.MWM_SIZE_PREDICTION_MODEL_SCALER_PATH, 'rb') as f:
            self.scaler = pickle.load(f)

    @classmethod
    def _get_instance(cls):
        if not hasattr(cls, '_instance'):
            cls._instance = cls()
        return cls._instance

    @classmethod
    def predict(cls, features_array):
        """1D or 2D array of feature values for predictions.
        Each feature is a list of values for factors
        defined by 'cls.factors' sequence.
        """
        X = np.array(features_array)
        one_prediction = (X.ndim == 1)
        if one_prediction:
            X = X.reshape(1, -1)

        predictor = cls._get_instance()
        X_scaled = predictor.scaler.transform(X)
        predictions = predictor.model.predict(X_scaled)
        if one_prediction:
            return predictions[0]
        else:
            return predictions.tolist()
