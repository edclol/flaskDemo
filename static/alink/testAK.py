# -*- coding: utf-8 -*-
"""
Created by edc on 2020/8/11
"""

from pyalink.alink import *
import sys, os

resetEnv()
useLocalEnv(2)

a = AkSourceBatchOp().setFilePath("hdfs://172.16.1.127:9000/data/als_example.csv")
a.print()