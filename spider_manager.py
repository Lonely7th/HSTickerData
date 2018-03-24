#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
__title__ = ''
__author__ = 'JN Zhang'
__mtime__ = '2018/3/22'
"""
import json
import multiprocessing
import os
import bs4
from time import sleep

import requests

from k_data.k_logs.logs_manager import LogsManager

base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))


# 从指定位置开始爬取数据
def start_spider(code_list):
    date_list = list(reversed([c for c in creat_date_list()]))
    for code in code_list:
        get_html(code, date_list)


# 获取页面内容
def get_html(tk_code, date_list):
    result_list = []
    for item_date in date_list:
        url = "http://quotes.money.163.com/trade/lsjysj_" + tk_code + ".html?year=" + str(item_date[0]) + "&season=" + str(item_date[1])
        print(url)
        # 请求失败后重新请求(最多8次)
        max_try = 8
        for tries in range(max_try):
            try:
                content = requests.get(url).content
                # 解析页面
                soup = bs4.BeautifulSoup(content, "lxml")
                parse_list = soup.select("div.inner_box tr")
                for item in parse_list[1:]:
                    data = [x.string for x in item.select("td")]
                    price = {
                        "cur_timer": data[0],
                        "cur_open_price": data[1],
                        "cur_max_price": data[2],
                        "cur_min_price": data[3],
                        "cur_close_price": data[4],
                        "cur_price_range": data[6],
                        "cur_total_volume": data[7],
                        "cur_total_money": data[8]
                    }
                    result_list.append(price)
                break
            except Exception:
                if tries < (max_try - 1):
                    sleep(2)
                    continue
                else:
                    logger.error("SpiderError：" + tk_code)
    # 保存数据
    __file = open("D:\_ticker_data\\" + str(tk_code) + ".txt", "w")
    __file.write(json.dumps(result_list))
    __file.close()


# 获取日期组合
def creat_date_list():
    for year in range(2007, 2018):
        for season in range(1, 5):
            yield (year, season)


# 获取股票代码列表
def creat_code_list():
    path_file_code = base_path + "/k_data/data_code.txt"
    file_code = open(path_file_code, "r", encoding='utf-8')
    while True:
        tk_code = file_code.readline()
        if "" == tk_code:
            break
        yield tk_code[:6]


if __name__ == "__main__":
    logger = LogsManager("logs_spider").get_logger()
    code_list = [item for item in creat_code_list()]
    pool = multiprocessing.Pool(processes=3)
    pool.apply_async(start_spider, args=(code_list[:1200], ))
    pool.apply_async(start_spider, args=(code_list[1200:2400], ))
    pool.apply_async(start_spider, args=(code_list[2400:], ))
    pool.close()
    pool.join()
