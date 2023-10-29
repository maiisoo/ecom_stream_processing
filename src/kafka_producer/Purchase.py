import numpy as np
from datetime import datetime


class Purchase:
    def __init__(self, user_id, product_id):
        self.user_id = user_id
        self.product_id = product_id
        self.datetime = None
        self.payment = None
        self.status = None
        self.error = None

    def randPurchase(self):
        self.datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        payments = ['Visa', 'MasterCard', 'COD', 'e-wallet']
        self.payment = np.random.choice(payments)
        self.status = np.random.choice(['successful', 'successful', 'successful', 'failed'])
        err = ['network connectity issue', 'invalid payment details', 'transaction declined', 'unknown']
        if (self.status == 'failed'):
            self.error = np.random.choice(err)


    def toMessage(self):
        self.randPurchase()
        return {
            "user_id": str(self.user_id),
            "product_id": str(self.product_id),
            "datetime": str(self.datetime),
            "payment":self.payment,
            "status":self.status,
            "error":self.error
        }


