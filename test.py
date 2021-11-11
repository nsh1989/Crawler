from Database.dbmanager import DBManager
from Crawler.encarCrawler import CrawlerEncar
import pandas as pd
import json
import datetime
import numpy as np
import dateutil
from dateutil import rrule

if __name__ == '__main__':
    deltatime = 1634966319000
    firstdate = datetime.datetime.fromtimestamp(deltatime / 1e3)
    # td = datetime.timedelta(deltatime)
    print("firstRgDt")
    print(firstdate)
    yearmon = "201106"
    date = dateutil.parser.parse(yearmon)
    print(date)
    regdate = datetime.datetime.strptime(yearmon, "%Y%m")
    print(date)
    date_diff = firstdate - regdate
    print(date_diff)
    diff_list = list(rrule.rrule(rrule.MONTHLY, dtstart=regdate,until=firstdate))
    print(diff_list)
    print(len(diff_list))
    solddt = datetime.datetime.strptime("20211110", "%Y%m%d")
    print(solddt)
    # 1632810801000

    # deltatime = 1634893151000
    # date = datetime.datetime.fromtimestamp(deltatime / 1e3)
    # print("regDt")
    # print(date)

    # print(dateutil.parser.parse(date))

    # df = pd.read_excel('./text/Attach②-1 VIN Lexus.xlsx')
    # print(df)
    # dbManager = DBManager()
    #
    # data = {
    #     'ID': 1234,
    #     'RAWDATA': {
    #         'test': {
    #             'test1':'test'
    #         },
    #         'test2':'test2'
    #     }
    # }
    # data['RAWDATA'] = json.dumps(data['RAWDATA'])
    # table='encarlist'
    # columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in data.keys())
    # values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in data.values())
    # sql = "INSERT INTO %s ( %s ) values ( %s );" % (table, columns, values)
    # print(sql)
    # dbManager.cursor.execute(sql)
    # dbManager.conn.commit()
    #
    # sql = "SELECT * From encarlist"
    # print(sql)
    # dbManager.cursor.execute(sql)
    # rows = [item for item in dbManager.cursor.fetchall()]
    # row_headers=[x[0] for x in dbManager.cursor.description]
    # json_data=[]
    # for result in rows:
    #     json_data.append(dict(zip(row_headers, result)))
    # print(type(json_data[0]['ID']))
    # data = json.loads(json_data[0]['RAWDATA'])
    # print(type(data['test']))
    # print(data['test2'])

    # crawler = CrawlerEncar()
    # crawler.ecodematch()
    # test = None
    #
    # if test is None:
    #     print('none')

    # ecode = pd.read_excel("./text/test.xlsx")
    # for index, row in ecode.iterrows():
    #     if len(row['vin']) != 17:
    #         continue
    #     try:
    #         ecode = int(row['ecode'])
    #     except Exception as e:
    #         print(e)
    #         continue
    #     dict = {}
    #     dict['Manufacturer'] = row['Manufacturer']
    #     dict['ecode'] = ecode
    #     dict['Badge'] = row['Badge']
    #     dict['BadgeDetail'] = row['BadgeDetail']
    #     dict['FormYear'] = row['FormYear']
    #     dict['FuelType'] = row['FuelType']
    #     dict['Transmission'] = row['Transmission']
    #     dict['Model'] = row['Model']
    #
    #     columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in dict.keys())
    #     values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in dict.values())
    #     sql = "INSERT INTO %s ( %s ) values ( %s );" % ('encarecode', columns, values)
    #     print(sql)
    #     dbManager.cursor.execute(sql)
    #     dbManager.conn.commit()


