from numpy import int64

from Database.dbmanager import DBManager
from Crawler.encarCrawler import CrawlerEncar
import pandas as pd
import json
import datetime
import numpy as np
import dateutil
from dateutil import rrule
from Database.dbmanager import DBManager
import numpy as np
import sqlalchemy
from dateutil import rrule
import datetime
import dateutil.parser


def countAge(purchaseDate, regDate):
    print(purchaseDate)
    print(regDate)
    diff_list = list(rrule.rrule(rrule.MONTHLY, dtstart=regDate, until=purchaseDate))
    print(len(diff_list))
    return len(diff_list)


def findEcodeDATtable(df, Model, Engine, Type):
    # result1 = df.query(f'Model={Model} and Engine={Engine} and Type={Type}')
    result = df[(df['Model'] == Model) &
                (df['Engine'] == Engine) &
                (df['Type'] == Type)]
    if result['eCode'].empty:
        return None
    return result['eCode'].values[0]


def findEcodeHistorytable(df, Model, Year):
    result = df[(df['Model'] == Model) &
                (df['Year'] == Year)]
    # if Model == "SLK 200" and Year == "2016":
    #     print(result['eCode'])
    #     return result['eCode'].values[0]
    if result['eCode'].empty:
        return None
    return result['eCode'].values[0]


def getDatDataFrame(filename):
    df1 = pd.read_excel(filename, sheet_name="Real cases on SD3", converters={'eCode': np.str})
    df1.rename(columns={'Order ID': 'OrderNumber', 'First registration': 'FirstRegDate',
                        '매입채널': 'tradeChannel', '한성자동차 매입가격 (입력)': 'purchasePrice',
                        '한성자동차 판매가격 (입력)': 'salesPrice', 'Created': 'purchaseDate', 'Age': 'Age'
                        }, inplace=True)

    eCode = df1.dropna(subset=['eCode'])
    eCode = eCode.drop_duplicates(subset=['eCode', 'Model', 'Engine', 'Type'])
    df1['eCode'] = df1.apply(
        lambda x: findEcodeDATtable(eCode, x['Model'], x['Engine'], x['Type']) if pd.isnull(x['eCode']) else x['eCode'],
        axis=1)

    df1['eCode'] = df1['eCode'].astype(str)
    df1['eCode'] = df1.apply(lambda x: x['eCode'] if len(x['eCode']) == 14 else None, axis=1)
    # df1['eCode'] = df1.apply(lambda x:  if np.isnan(x['eCode']) else None, axis=1)
    df1 = df1.dropna(subset=['eCode'])
    df1['eCode'] = df1.apply(lambda x: x['eCode'].zfill(15), axis=1)

    df1.to_excel('test.xlsx')

    return df1


def getHitoryDataFrame(filename):
    df2 = pd.read_excel(filename, sheet_name="Historic transaction", dtype={'€-Code': str})

    df2.rename(columns={'No.': 'OrderNumber', 'Registration Date': 'FirstRegDate', '€-Code': 'eCode',
                        'Purchase Channel': 'tradeChannel', 'Purchase Price\n(excl. VAT)': 'purchasePrice',
                        'Re-sales Price\n(excl. VAT)': 'salesPrice', 'VIN No.': 'VIN',
                        'Retail Price\n(excl. VAT)': 'retailPrice',
                        'Period.1': 'Age', 'Purchase Date': 'purchaseDate', 'Re-sales Date': 'soldDate',
                        'Mileage': 'km',
                        'Type Class': 'Type'
                        }, inplace=True)
    df2['Year'] = df2['Year'].astype(str)
    df2['Model'] = df2['Model'].astype(str)
    eCode = df2.dropna(subset=['eCode'])
    eCode = eCode.drop_duplicates(subset=['eCode', 'Model', 'Year'])
    eCode['Year'] = eCode['Year'].astype(str)
    eCode['Model'] = eCode['Model'].astype(str)
    # eCode 없는 컬럼 확인
    # eCode 없을 시 findEcodeDATtable
    df2['eCode'] = df2.apply(
        lambda x: findEcodeHistorytable(eCode, x['Model'], x['Year']) if pd.isnull(x['eCode']) else x['eCode'],
        axis=1)
    # 15자리 변경
    df2.to_excel('beforeNa.xlsx')
    df2 = df2.dropna(subset=['eCode'])
    df2['eCode'] = df2['eCode'].astype(str)
    drop_row = df2[df2['eCode'].str.len() < 15].index
    print(f"df2 drop 15 전  {len(df2)}   {drop_row}")
    for i in df2['eCode']:
        if len(i) < 15:
            print(i)

    df2 = df2.drop(df2[df2['eCode'].str.len() < 15].index)
    print(f"df2 drop 15 후  {len(df2)}")
    df2['eCode'] = df2.apply(lambda x: x['eCode'] if len(x['eCode']) == 15 else x['eCode'][:15], axis=1)
    eCode.to_excel('df2_ecode.xlsx')
    df2.to_excel('afterNa.xlsx')

    return df2


if __name__ == '__main__':
    dbManager = DBManager()

    df1 = getDatDataFrame(filename="./files/Han sung data.xlsx")
    df2 = getHitoryDataFrame(filename="./files/Han sung data.xlsx")

    print("히스토리 데이터, DAT 입력 데이터 필요없는 컬럼 제거")
    col1 = set(df1.columns.tolist())
    col2 = set(df2.columns.tolist())
    df1 = df1.drop(col1.difference(col2), axis=1)
    df2 = df2.drop(col2.difference(col1), axis=1)

    result = pd.concat([df1, df2])
    print(f"합친 후 데이터 길이 f{len(result)}")

    print(f"Trading-Data 정제 시작 na = 일반 구매 변경")
    tradeChannel = {
        "Trade-in": 1,
        "Demo": 2,
        "Universal-purchase": 3,
    }
    result = result.replace({'tradeChannel': tradeChannel})
    result['tradeChannel'] = result['tradeChannel'].fillna(3)

    result = result.dropna(subset=['purchaseDate', 'FirstRegDate', 'purchasePrice', 'salesPrice', 'eCode'])
    result['Age'] = result.apply(
        lambda x: countAge(x['purchaseDate'], x['FirstRegDate']) if np.isnan(x["Age"]) else x['Age'], axis=1)

    result['purchasePrice'] = result['purchasePrice'].fillna(0).replace(',', '').astype(int)
    result['salesPrice'] = result['salesPrice'].fillna(0).replace(',', '').astype(int)
    result['km'] = result['km'].fillna(0).replace(',', '').astype(int)
    result['purchaseDate'] = pd.to_datetime(result['purchaseDate'])
    result['FirstRegDate'] = pd.to_datetime(result['FirstRegDate'])

    engine = sqlalchemy.create_engine(
        # "mysql+mysqlconnector://root:root@13.209.18.171:3306/crawler", encoding='utf8'
        "mysql+mysqlconnector://root:root@localhost:3306/crawler", encoding='utf8'
    )
    conn = engine.connect()

    result.to_excel("result.xlsx")
    result.to_sql(name='hansung', con=conn, if_exists='replace', index=False)
    #
    # conn.close()
    # for cols in col1:
    #     if col2.find(cols) == False:
    #         df1 = df1.drop(cols, axis=1)
    # df2.rename(columns={'Order ID': 'OrderNumber', 'First registration': 'FirstRegDate',
    #                     '매입채널': 'tradeChannel', '한성자동차 매입가격 (입력)': 'purchasePrice',
    #                     '한성자동차 판매가격 (입력)': 'salesPrice',
    #                     }, inplace=True)
    # deltatime = 1634966319000
    # firstdate = datetime.datetime.fromtimestamp(deltatime / 1e3)
    # # td = datetime.timedelta(deltatime)
    # print("firstRgDt")
    # print(firstdate)
    # yearmon = "201106"
    # date = dateutil.parser.parse(yearmon)
    # print(date)
    # regdate = datetime.datetime.strptime(yearmon, "%Y%m")
    # print(date)
    # date_diff = firstdate - regdate
    # print(date_diff)
    # diff_list = list(rrule.rrule(rrule.MONTHLY, dtstart=regdate,until=firstdate))
    # print(diff_list)
    # print(len(diff_list))
    # solddt = datetime.datetime.strptime("20211110", "%Y%m%d")
    # print(solddt)
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
