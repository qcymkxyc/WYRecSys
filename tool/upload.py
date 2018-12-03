#!usr/bin/env python
# -*- coding:utf-8 _*-

"""
    @author:qcymkxyc
    @email:qcymkxyc@163.com
    @software: PyCharm
    @file: upload.py
    @time: 2018/12/1 15:30

    上传playlist数据
"""
from pymongo import MongoClient
from pymongo.errors import AutoReconnect
import json

host = "180.76.53.98"
port = 27017


def read_file(filename):
    """读取文件

    :param filename:str
        文件名
    :return: str
        一行文字
    """
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            yield line


if __name__ == "__main__":
    filename = r"F:\mystyle\learning\七月在线推荐系统\popular.playlist"

    conn = MongoClient(host, port)
    db = conn.wy_playlist
    playlist = db.playlist

    skip_row = 0
    for i, line in enumerate(read_file(filename)):
        if i <= skip_row:
            continue

        print("正在上传第{}条".format(i))
        line_json = json.loads(line)
        try:
            playlist.save(line_json)
        except AutoReconnect:
            continue
