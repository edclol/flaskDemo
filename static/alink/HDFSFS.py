# -*- coding: utf-8 -*-
"""
Created by edc on 2020/8/11
"""
# -*- coding: utf-8 -*-
"""

"""

import os
import hdfs, json, pickle
# 使用pickle而不用joblib，是因为考虑到不仅仅sklearn模型保存以及pickle可以直接保存到内存中
import pandas as pd

WebHDFSAddr = "http://172.16.1.127:50070"
localOutputPathTemp = '/tmp/hdfs'
localInputPathTemp = '/tmp/hdfs'
NginxAddr = "http://172.16.1.23:22222"


class HDFSFS(object):
    def __init__(self):
        # 对于WebHDFS的读写，在不考虑Kuberos的情况下，需要将/etc/hosts文件配置和Hadoop集群一样的IP
        self.HDFSClient = hdfs.InsecureClient(WebHDFSAddr, root='/', user="root")
        self.localInputPathTemp = localInputPathTemp
        self.localOutputPathTemp = localOutputPathTemp
        self.NginxAddr = NginxAddr

    @staticmethod
    def type2str(x):
        if x.startswith('float'):
            return 'double'
        elif x.startswith('int'):
            return 'int'
        elif x.startswith('object'):
            return 'string'
        elif x.startswith('datetime'):
            return 'datetime'
        elif x.startswith('bool'):
            return 'bool'
        else:
            return 'object'

    # 读取数据
    def load_dataframe(self, inputPath):
        #  "hdfs:/user/experiment/tmp/{graph_id}-{node_id}{port_id}" 要去掉前面的hdfs
        inputPath = inputPath.split(':')[1]
        total = None
        if self.HDFSClient.status(inputPath)['type'] == 'FILE':  # 是sklearn生成的
            with self.HDFSClient.read(inputPath) as fs:
                print(inputPath)
                print(type(fs))
                # 加engine = 'python'会报错
                total = pd.read_csv(fs, encoding='utf-8')
        elif self.HDFSClient.status(inputPath)['type'] == 'DIRECTORY':  # 是spark生成的
            first = True
            for i in self.HDFSClient.list(inputPath):
                if i.startswith('part'):
                    with self.HDFSClient.read(os.path.join(inputPath, i)) as fs:
                        tmp = pd.read_csv(fs, encoding='utf-8', engine='python')
                        if first:
                            total = tmp
                            first = False
                        else:
                            total = pd.concat([total, tmp], axis=0)
        total.index = range(total.shape[0])
        return total

    # 读取数据和schema
    def load_dataframe_schema(self, inputPath):
        total = self.load_dataframe(inputPath)
        schema = total.dtypes.apply(lambda x: HDFSFS.type2str(str(x))).to_dict()
        return total, schema

    # alink
    # 读取数据和schema
    def hd2df_alink(self, inputPath):
        """

        @param inputPath: example hdfs:/data/1.csv 输入的文件必须是csv 头行作为标题行 标题行不能为数字 不能有特殊符号 不然alink不认
        @return:dataframe 和 alink的字符串schema
        """
        total = self.load_dataframe(inputPath)
        schema = total.dtypes.apply(lambda x: HDFSFS.type2str(str(x))).to_dict()
        # schema
        strR = ""
        for k, v in schema.items():
            strR += k + " " + v + ","

        return total, strR[:-1]

    def putFile2hd(self, local_path, hdfs_path ):
        self.HDFSClient.upload(hdfs_path=hdfs_path, local_path=local_path, overwrite=True)

    # 存储json
    def save_json(self, data, outputPath, suffix=''):
        # 目前的outputPath ： "hdfs:/user/experiment/tmp/{graph_id}-{node_id}{port_id}"
        graphNodePort = outputPath.split('/')[-1]
        localOutputPath = os.path.join(self.localOutputPathTemp, graphNodePort) + suffix
        outputPath = outputPath + suffix
        #  "hdfs:/user/experiment/tmp/{graph_id}-{node_id}{port_id}" 要去掉前面的hdfs
        outputPath = outputPath.split(':')[1]
        with open(localOutputPath, 'w') as f:
            f.write(json.dumps(data))
        self.HDFSClient.upload(outputPath, localOutputPath, overwrite=True)

    # 存储数据
    def save_dataframe(self, data, outputPath):
        # 用来做下面schema的转换
        def type2str(x):
            if x.startswith('float'):
                return 'float'
            elif x.startswith('int'):
                return 'int'
            elif x.startswith('object'):
                return 'str'
            elif x.startswith('datetime'):
                return 'datetime'
            elif x.startswith('bool'):
                return 'bool'
            else:
                return 'object'

        # 目前的outputPath ： "hdfs:/user/experiment/tmp/{graph_id}-{node_id}{port_id}"
        graphNodePort = outputPath.split('/')[-1]
        localOutputPath = os.path.join(self.localOutputPathTemp, graphNodePort)
        #  "hdfs:/user/experiment/tmp/{graph_id}-{node_id}{port_id}" 要去掉前面的hdfs
        realOutputPath = outputPath.split(':')[1]
        # 一定要注意不能加index！否则下次读取会多一列Unnamed的index列
        data.to_csv(localOutputPath, index=None)
        self.HDFSClient.upload(realOutputPath, localOutputPath, overwrite=True)
        file = {}
        file['fileTime'] = self.HDFSClient.status(realOutputPath)['modificationTime']
        file['fileType'] = "DataFrame"
        file['fileShape'] = data.shape
        # {'CRIM': 'float', 'ZN': 'float'}
        file['fileSchema'] = data.dtypes.apply(lambda x: type2str(str(x))).to_dict()
        self.save_json(file, outputPath, '_stat')
