import concurrent.futures

import pymysql
from Utils.SingletonInstance import SingleTonInstance


class DBManager(SingleTonInstance):
    conn = pymysql.connect(host='localhost', user='root', password='root', db='crawler', charset='utf8')
    cursor = conn.cursor()
    def __init__(self):
        sql = """CREATE TABLE IF NOT EXISTS ENCARLIST(ID VARCHAR(255), MANUFACTURER VARCHAR(255), MODEL VARCHAR(255), BADGE VARCHAR(255), BADGEDETAIL VARCHAR(255), FUELTYPE VARCHAR(255), TRANSMISSION VARCHAR(255), FORMYEAR VARCHAR(255), MILEAGE VARCHAR(255), PRICE VARCHAR(255), PHOTO VARCHAR(255), SERVICECOPYCAR VARCHAR(255), UPDATEDDATE DATE, ORGID VARCHAR(255), URL VARCHAR(255), VIN VARCHAR(255), ACCIDENT VARCHAR(255), REPAIR VARCHAR(255), SELLERID VARCHAR(255), SOLDDATE DATE, YearMon INT, ModifiedDate DATE )"""
        self.cursor.execute(sql)
        # self.conn.commit()
        # self.cursor.close()
        # self.conn.close()
        # try:
        #     with self.cursor as cur:
        #         cur.execute(sql)
        #     self.conn.commit()
        # finally:
        #     self.conn.close()
        # pass
