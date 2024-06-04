# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

class DataToNeo4jSpark(object):
    """将excel中数据存入neo4j"""
    
    url = "bolt://localhost:7687"
    username = "neo4j"
    password = "12345678"
    invoice_name = '发票名称'
    invoice_value = '发票值'

    def __init__(self):
        """建立连接"""
        self.spark = (
            SparkSession.builder.config("neo4j.url", self.url)
            .config("neo4j.authentication.basic.username", self.username)
            .config("neo4j.authentication.basic.password", self.password)
            .getOrCreate()
        )

    def create_node(self, node_list_key, node_list_value):
        """建立节点"""
        data = []
        for key in node_list_key:
            data.append({"name": key})
        for key in node_list_value:
            data.append({"name": key})
        self.df = self.spark.createDataFrame(data)

    def write_to_neo4j(self):
        self.df.write.format("org.neo4j.spark.DataSource").mode("Overwrite").option("labels", self.invoice_name).option("node.keys", "name").save()

    def read_from_neo4j(self):
        self.spark.read.format("org.neo4j.spark.DataSource").option("labels", self.invoice_name).load().show()