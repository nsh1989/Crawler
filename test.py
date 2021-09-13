from Database.dbmanager import DBManager
from Crawler.encarCrawler import CrawlerEncar
import pandas as pd

if __name__ == '__main__':
    # df = pd.read_excel('./text/Attach②-1 VIN Lexus.xlsx')
    # print(df)
    dbManager = DBManager()
    crawler = CrawlerEncar()
    crawler.ecodematch()
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


