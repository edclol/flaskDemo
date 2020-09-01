# -*- coding: utf-8 -*-
"""
Created by edc on 2020/8/11
"""
# -*- coding: utf-8 -*-
from HDFSFS import HDFSFS

"""
Created by edc on 2020/8/10
"""

from pyalink.alink import *
import sys, os

resetEnv()
useLocalEnv(2)


@staticmethod
def alss(inPath="hdfs:/data/als_example.csv", outPath="hdfs:/data/als_modle.csv", useCol="user", itemCol="item",
         rateCol="rating", preCol="pred_rating", numIter=10, rank=10, Lambda=0.01, ):
    HDFSServer = HDFSFS()
    df_data, schema = HDFSServer.hd2df_alink(inPath)

    data = dataframeToOperator(df_data, schemaStr=schema, op_type='batch')

    als = ALS().setUserCol(useCol).setItemCol(itemCol).setRateCol(rateCol) \
        .setNumIter(numIter).setRank(rank).setLambda(Lambda).setPredictionCol(preCol)

    # pipline = Pipeline()
    # pipline.add(als)
    #
    # piplineModel = pipline.fit(data)
    # piplineModel.save("als_modle.csv")
    # HDFSServer.putFile2hd("als_modle.csv",hdfs_path="/data/als_modle.csv")

    pred = als.fit(data).transform(data)
    pred.print()

    return ""

    return ""
