
import os
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages com.crealytics:spark-excel_2.11:0.8.2 pyspark-shell")


class SparkContextSC:
    def __init__(self):
        self.spark = SparkSession.builder.appName("CheckPoint5").master("local[*]").config("spark.driver.memory", "6g").getOrCreate()
        self.sc = self.spark.sparkContext
        self.sql_context = SQLContext(self.sc)


class JarDownload(object):

    def __init__(self, sql_context):
        self.sql_context = sql_context

    def read_excel(self):
        df = self.sql_context.read.format("com.crealytics.spark.excel") \
                    .option("useHeader", "true") \
                    .option("treatEmptyValuesAsNulls", "true") \
                    .option("inferSchema", "true") \
                    .option("location", "NPO_DASHBOARD_BSC_DAY-NOKBSC-BSC-day-PM_20191118TestMail11111.xlsx") \
                    .option("addColorColumns", "False") \
                    .option("sheetName", "Sheet1") \
                    .load("Daily_FMT_RAN_Report_01052019.xlsb")
        df.show()


if __name__ == "__main__":
    sql_context_ = SparkContextSC().sql_context
    file_path = "CheckPoint4Input.txt"

    obj = JarDownload(sql_context_)
    obj.read_excel()
