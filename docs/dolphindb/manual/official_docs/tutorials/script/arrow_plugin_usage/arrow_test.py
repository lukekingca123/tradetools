from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql as sql
import pyspark.sql.functions as F

import dolphindb as ddb
import dolphindb.settings as keys

import pandas as pd
import pyarrow as pa

import time

import os
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"


HOST = "localhost"
PORT = 8848
USER = "admin"
PASSWD = "123456"

script = """
    n = 10000000;
    id = take(`AAA`BBB`CCC, n);
    v1 = rand(10.0, n);
    v2 = rand(20.0, n);
    v3 = rand(5.0, n);
    share table(id as id, v1 as v1, v2 as v2, v3 as v3) as t;
"""

conn = ddb.Session()
conn.connect(HOST, PORT, USER, PASSWD)
conn.run(script)


class _SparkBase(object):
    @classmethod
    def start(cls, use_arrow=True):
        conf = SparkConf()
        conf.set("spark.sql.execution.arrow.pyspark.enabled", "true" if use_arrow else "false")
        conf.set("spark.default.parallelism", "100")
        cls.context: SparkContext = SparkContext(master='local[*]', appName=cls.__name__, conf=conf)
        cls.session: SparkSession = SparkSession.builder.getOrCreate()
        cls.use_arrow = use_arrow

    @classmethod
    def shutdown(cls):
        cls.session.stop()
        cls.session = None
        cls.context.stop()
        cls.context = None


def timer(fn):
    def wrapper(*args, **kwargs):
        t1 = time.perf_counter()
        fn(*args, **kwargs)
        t2 = time.perf_counter()
        return t2 - t1
    return wrapper


def cal(df: sql.DataFrame):
    df = df.withColumn("v", (F.col("v1") + F.col("v2")) * F.col("v3"))
    df = df.groupby("id").avg("v")
    return df


class PySparkTest(_SparkBase):
    def testPROTOCOL_PICKLE(self):
        s = ddb.Session(protocol=keys.PROTOCOL_PICKLE)
        s.connect(HOST, PORT, USER, PASSWD)

        @timer
        def test(s: ddb.Session):
            df = s.run("t")
            df = self.session.createDataFrame(df)
            cal(df)

        return test(s)

    def testPROTOCOL_ARROW(self):
        s = ddb.Session(protocol=keys.PROTOCOL_ARROW)
        s.connect(HOST, PORT, USER, PASSWD)

        @timer
        def test(s: ddb.Session):
            pt = s.run("t")
            if not self.use_arrow:
                df = pt.to_pandas()
            else:
                df = pd.DataFrame({
                    'id': pd.Series(pt["id"].combine_chunks(), dtype=pd.ArrowDtype(pa.utf8())),
                    'v1': pd.Series(pt["v1"].combine_chunks(), dtype=pd.ArrowDtype(pa.float64())),
                    'v2': pd.Series(pt["v2"].combine_chunks(), dtype=pd.ArrowDtype(pa.float64())),
                    'v3': pd.Series(pt["v3"].combine_chunks(), dtype=pd.ArrowDtype(pa.float64())),
                })
            df = self.session.createDataFrame(df)
            cal(df)

        return test(s)


if __name__ == "__main__":
    # test without Arrow
    PySparkTest.start(use_arrow=False)
    t1 = PySparkTest().testPROTOCOL_PICKLE()
    print("PICKLE without Arrow: ", t1)
    t2 = PySparkTest().testPROTOCOL_ARROW()
    print("ARROW without Arrow: ", t2)
    PySparkTest.shutdown()

    # test with Arrow
    PySparkTest.start(use_arrow=True)
    t3 = PySparkTest().testPROTOCOL_PICKLE()
    print("PICKLE with Arrow: ", t3)
    t4 = PySparkTest().testPROTOCOL_ARROW()
    print("ARROW with Arrow: ", t4)
    PySparkTest.shutdown()
