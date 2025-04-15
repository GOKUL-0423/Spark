# Databricks notebook source
try:
    dbutils.fs.rm('dbfs:/user/hive/warehouse/',True)
except Exception as e:
    print('Failed')