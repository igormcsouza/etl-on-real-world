from datetime import datetime

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


get_year = udf(
    lambda x: datetime.strptime(x, '%d/%m/%Y %H:%M:%S').year if x else None, 
    IntegerType()
)

get_month = udf(
    lambda x: datetime.strptime(x, '%d/%m/%Y %H:%M:%S').month if x else None, 
    IntegerType()
)