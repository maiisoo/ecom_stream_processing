import pandas as pd
import numpy as np


class DataAccessObject:
    def __init__(self):
        self.users = pd.read_csv('../../data/users.csv', encoding='utf-8')
        self.products = pd.read_csv('../../data/products.csv', encoding='latin1')

    def getProductIDs(self):
        return np.array(self.products['id'])

    def getUserIDs(self):
        return np.array(self.users['user_id'])


dao = DataAccessObject()