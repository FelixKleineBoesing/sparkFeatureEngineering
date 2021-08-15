from feateng.data_input.table import Sales
from feateng.helpers import start_spark
from pyspark.sql import Row
import unittest


class SalesTester(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark_session = start_spark()[0]
        local_records = [
            Row(id=1, first_name='Dan', second_name='Germain', department=1),
            Row(id=2, first_name='Dan', second_name='Sommerville', department=1),
            Row(id=3, first_name='Alex', second_name='Ioannides', department=2),
            Row(id=4, first_name='Ken', second_name='Lai', department=2),
            Row(id=5, first_name='Stu', second_name='White', department=3),
            Row(id=6, first_name='Mark', second_name='Sweeting', department=3),
            Row(id=7, first_name='Phil', second_name='Bird', department=4),
            Row(id=8, first_name='Kim', second_name='Suter', department=4)
        ]
        df = cls.spark_session.createDataFrame(local_records)
        df.createTempView("sales")

    def test_construction(self):
        sales = Sales(spark_session=self.spark_session)

