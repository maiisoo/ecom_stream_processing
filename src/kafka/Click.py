import numpy as np
from src.kafka.dao import dao
from datetime import datetime


class Click:
    def __init__(self):
        self.user_id = None
        self.product_id = None
        self.source = None
        self.datetime = None

    def randClick(self):
        self.user_id = np.random.choice(dao.getUserIDs())
        self.product_id = np.random.choice(dao.getProductIDs())
        sources = ['Facebook', 'Youtube', 'Tiktok', 'Google', 'Gmail', 'Unknown']
        self.source = np.random.choice(sources)
        self.datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def toMessage(self):
        self.randClick()
        return {
            "user_id": str(self.user_id),
            "product_id": str(self.product_id),
            "source": self.source,
            "datetime": str(self.datetime)
        }
