# -*- coding: utf-8 -*-
"""
Created by edc on 2020/8/10
"""

# -*- coding: utf-8 -*-
"""
Created by edc on 2020/8/10
"""
from HDFSFS import *
from pyalink.alink import *
import sys, os

resetEnv()
useLocalEnv(3)


def alss(inPath="hdfs:/data/als_example.csv", outPath="hdfs:/data/als_modle.csv", useCol="user", itemCol="item",
         rateCol="rating", preCol="pred_rating", numIter=10, rank=10, Lambda=0.01, ):
    HDFSServer = HDFSFS()
    df_data, schema = HDFSServer.hd2df_alink(inPath)

    data = dataframeToOperator(df_data, schemaStr=schema, op_type='batch')

    als = ALS().setUserCol(useCol).setItemCol(itemCol).setRateCol(rateCol) \
        .setNumIter(10).setRank(10).setLambda(0.01).setPredictionCol(preCol)

    #     pred = als.fit(data).transform(data)
    #     pred.print()

    a = Pipeline().add(als).fit(data)

    a.transform(data).print()
    a.save("/home/edc/aa.csv")
    # ALSModel
    #     print(type(a))
    #     pipline = Pipeline().add(als).fit(data)

    #     pipline.save("/home/xll")

    # #     pipline.save("als_modle.csv")

    # HDFSServer.putFile2hd("iris.csv", hdfs_path="/data/als_modle.csv")

    return ""


alss()