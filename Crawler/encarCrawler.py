import datetime
import dateutil.parser
import sys
import pandas as pd
import pymysql.cursors
import schedule
import json
from dateutil import rrule

from Crawler.baseCrawler import CrawlerBase
from Crawler.baseCrawler import html
from Utils.datAPI import cdatapi


class CrawlerEncar(CrawlerBase):
    Scheduler = schedule
    totalCount = 0
    dat = cdatapi()

    def insertTable(self, dict, table):
        columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in dict.keys())
        values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in dict.values())
        sql = "INSERT INTO %s ( %s ) values ( %s );" % (table, columns, values)
        print(sql)
        self.dbManager.cursor.execute(sql)
        self.dbManager.conn.commit()

    def updateTable(self, dict, table, id):
        columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in dict.keys())
        values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in dict.values())
        set = """,""".join("""{}='{}'""".format(k, v) for k, v in dict.items())
        sql = "UPDATE  %s SET %s WHERE ID = %s;" % (table, set, id)
        print(sql)
        self.dbManager.cursor.execute(sql)
        self.dbManager.conn.commit()

    def make_sr(self, num):
        sr_param = "|ModifiedDate|" + str(num) + "|100"
        return sr_param

    def orgCarID(self, photopath):
        ID = str(photopath).split('/')
        ID = ID[len(ID)-1]
        ID = ID.replace('_', '')
        return ID

    def checkID(self, id):
        sql = "SELECT * FROM encarlist WHERE ID= %s;" % (id)
        self.dbManager.cursor.execute(sql)
        rows = self.dbManager.cursor.fetchall()
        if len(rows) > 0:
            print(id, len(rows))
            return True
        else:
            return False

    def strToDate(self,str):
        return dateutil.parser.parse(str)

    def ChangeExistance(self, id):
        sql = "UPDATE encarlist SET CHECKDETAIL = 1, BAVAILABLE = 0 where ID = %s " % (id)
        self.dbManager.cursor.execute(sql)

    def setup(self):
        self.param = {
        'count': 'ture',
        'q': '(And.Hidden.N._.CarType.N._.Condition.Inspection._.Condition.Record.)',
        'sr': self.make_sr(0)
        }
        self.url = 'http://api.encar.com/search/car/list/premium'

        resp = self.s.get(self.url, params=self.param)
        self.totalCount = resp.json()['Count']
        items = resp.json()['SearchResults']

        print(self.totalCount)
        print(len(items))

        self.Scheduler.every(1).week.do(self.detailProcess)
        self.Scheduler.every(2).weeks.do(self.getlist)

    def run(self):
        while True:
            self.Scheduler.run_all()

    def getlist(self):
        print("start get list")
        pages = self.totalCount / 100
        if self.totalCount % 100 > 0:
            pages = pages + 1
        for page in range(0, int(pages)):
            self.param['sr'] = self.make_sr(page * 100)
            resp = self.s.get(self.url, params=self.param).json()['SearchResults']
            for item in resp:
                dict = {}
                if item['ServiceCopyCar'] == 'DUPLICATION':
                    continue
                if self.checkID(item['Id']):
                    continue
                dict['Id'] = item['Id']
                dict['Manufacturer'] = item['Manufacturer']
                dict['Model'] = item['Model']
                dict['Badge'] = str(item['Badge']).replace("'", "''")
                dict['BadgeDetail'] = ''
                try:
                    dict['BadgeDetail'] = str(item['BadgeDetail']).replace("'", "''")
                except Exception as e:
                    print(f"BadgeDetail : {e}")
                    pass
                dict['FuelType'] = item['FuelType']
                dict['Transmission'] = item['Transmission']
                dict['FormYear'] = item['FormYear']
                dict['FIRSTREG'] = item['Year']
                dict['Mileage'] = item['Mileage']
                dict['Price'] = item['Price']

                self.insertTable(dict, 'encarlist')
            print(page)

    def carDetail(self, data):
        data = data['inspect']
        uploadData = {}

        rawdata = json.dumps(data)
        uploadData['RAWDATA'] = rawdata
        uploadData['CHECKDETAIL'] = 1
        # 매물최초등록일
        try:
            date = data['carSaleDto']['firstRegDt']['time']
            date = datetime.datetime.fromtimestamp(date / 1e3)
            uploadData['UPDATEDDATE'] = date
        except Exception as e:
            print("매물 최초등록일")
            print(e)
        # 매물변경일
        try:
            date = data['carSaleDto']['mdfDt']['time']
            date = datetime.datetime.fromtimestamp(date / 1e3)
            uploadData['ModifiedDate'] = date
        except Exception as e:
            print("매물 변경일")
            print(e)
        # 사고이력
        try:
            uploadData['ACCIDENT'] = data['direct']['inspectAccidentSummary']['accident']['code']
        except Exception as e:
            print("사고이력")
            print(e)
        # 수리이력
        try:
            uploadData['REPAIR'] = data['direct']['inspectAccidentSummary']['simpleRepair']['code']
        except Exception as e:
            print("수리이력")
            print(e)
        # vin
        try:
            if len(data['direct']['master']['carregiStration']) == 17:
                uploadData['VIN'] = data['direct']['master']['carregiStration']
        except Exception as e:
            print("VIN")
            print(e)
        # AGE
        try:
            startDate = data['carSaleDto']['year']
            startDate = datetime.datetime.strptime(startDate, "%Y%m")
            endDate = uploadData['UPDATEDDATE']
            diff_list = list(rrule.rrule(rrule.MONTHLY, dtstart=startDate, until=endDate))
            uploadData['AGE'] = len(diff_list)
        except Exception as e:
            print("AGE")
            print(e)
        # SOLDDATE
        try:
            date = data['carSaleDto']['soldDt']['time']
            solddt = datetime.datetime.strptime(date, "%Y%m%d")
            uploadData['SOLDDATE'] = str(solddt)
            date = uploadData['SOLDDATE'] - uploadData['UPDATEDDATE']
            uploadData['SOLDDAYS'] = date
        except Exception as e:
            print("SOLDDATE")
            print(e)

        return uploadData

    def checkDetail(self, carID, sdFlag = 'N'):
        url = f'http://www.encar.com/dc/dc_cardetailview.do'
        req = {
            'method': 'ajaxInspectView',
            'sdFlag': sdFlag,
            'rgsid': carID
        }
        resp = self.s.get(url, params=req)
        data = resp.json()[0]
        if data['msg'] == 'FAIL':
            self.ChangeExistance(carID)
            return {}

        return self.carDetail(data)

    def detailProcess(self):
        # -- 디테일 정보 확인 안한 정보 테이블에서 가져오기
        print("start get details")
        sql = "SELECT * From encarlist WHERE CHECKDETAIL is FALSE AND BAVAILABLE is TRUE"
        self.dbManager.cursor.execute(sql)
        rows = [item for item in self.dbManager.cursor.fetchall()]
        # Json Type으로 변환
        row_headers = [x[0] for x in self.dbManager.cursor.description]
        json_data = []
        for result in rows:
            json_data.append(dict(zip(row_headers, result)))

        # 팔리지 않은 차량에 대한 정보 확인 URL 및 Params sdFlag = Y 일시 팔린 차량에 대한 정보
        for row in json_data:
            carID = row['ID']
            uploadData = self.checkDetail(carID=carID, sdFlag='N')
            if uploadData == {}:
                continue
            self.updateTable(uploadData, 'encarlist', carID)

        sql = "SELECT * From encarlist WHERE CHECKDETAIL is TRUE AND BAVAILABLE is TRUE"
        self.dbManager.cursor.execute(sql)
        rows = [item for item in self.dbManager.cursor.fetchall()]
        # Json Type으로 변환
        row_headers = [x[0] for x in self.dbManager.cursor.description]
        json_data = []
        for result in rows:
            json_data.append(dict(zip(row_headers, result)))

        for row in json_data:
            carID = row['ID']
            uploadData = self.checkDetail(carID=carID, sdFlag='Y')
            if uploadData == {}:
                continue
            self.updateTable(uploadData, 'encarlist', carID)


    # def test(self):
    #
    #     sql = """SELECT *, COUNT(*)
    #     FROM encarlist WHERE VIN is not null GROUP BY MANUFACTURER, MODEL, BADGE, BADGEDETAIL, FUELTYPE, TRANSMISSION, FORMYEAR
    #     HAVING COUNT(*) > 0;"""
    #     cursor = self.dbManager.conn.cursor(pymysql.cursors.DictCursor)
    #     cursor.execute(sql)
    #     dict = cursor.fetchall()
    #
    #     # columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in dict.keys())
    #     # values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in dict.values())
    #     # set = """,""".join("""{}='{}'""".format(k, v) for k, v in dict.items())
    #     # sql = "UPDATE  %s SET %s WHERE ID = %s;" % (table, set, id)
    #     # print(sql)
    #     # self.dbManager.cursor.execute(sql)
    #     # self.dbManager.conn.commit()
    #     dat = cdatapi()
    #
    #     for item in dict:
    #         src = {}
    #         src['badge'] = item['BADGE']
    #         src['badgedetail'] = item['BADGEDETAIL']
    #         src['manufacturer'] = item['MANUFACTURER']
    #         src['model'] = item['MODEL']
    #         src['fueltype'] = item['FUELTYPE']
    #         src['transmission'] = item['TRANSMISSION']
    #         src['formyear'] = item['FORMYEAR']
    #         where = """and """.join("""{}='{}'""".format(k, v) for k, v in src.items())
    #
    #         sql = """
    #         SELECT COUNT(*) FROM encarecode
    #         WHERE %s HAVING COUNT(*) > 0;
    #         """ % (where)
    #         self.dbManager.cursor.execute(sql)
    #         row = self.dbManager.cursor.fetchall()
    #
    #         if len(row) > 0:
    #             print('encarCrawler : ecode exist')
    #             continue
    #
    #         ecode = self.ecodesearch(item['VIN'])
    #         if ecode is None:
    #             print("encarCrawler : ecode : None")
    #             continue
    #         print(ecode)
    #         src['ecode'] = ecode
    #         self.insertTable(src, 'encarecode')
    #         print(f'encarCrawler : {ecode} inserted')
    #
    # def ecodesearch(self, vin):
    #     ecode = self.dat.getEcodeByVin(vin)
    #     return ecode
    #
    # def ecodematch(self):
    #
    #     sql = """
    #     SELECT * FROM encarlist;
    #     """
    #     cursor = self.dbManager.conn.cursor(pymysql.cursors.DictCursor)
    #     cursor.execute(sql)
    #     dict = cursor.fetchall()
    #     list = []
    #     for item in dict:
    #         src = {}
    #         src['badge'] = item['BADGE'].replace("'", "''")
    #         src['badgedetail'] = item['BADGEDETAIL'].replace("'", "''")
    #         src['manufacturer'] = item['MANUFACTURER']
    #         src['model'] = item['MODEL']
    #         src['fueltype'] = item['FUELTYPE']
    #         src['transmission'] = item['TRANSMISSION']
    #         src['formyear'] = item['FORMYEAR']
    #         where = """and """.join("""{}='{}'""".format(k, v) for k, v in src.items())
    #
    #         sql = """
    #         SELECT ecode, COUNT(*) FROM encarecode
    #         WHERE %s HAVING COUNT(*) > 0;
    #         """ % (where)
    #         self.dbManager.cursor.execute(sql)
    #         row = [item[0] for item in self.dbManager.cursor.fetchall()]
    #
    #         ecode = ''
    #         if len(row) < 1:
    #             print('encarCrawler ecodematch : ecode not exist')
    #         else:
    #             print(f'encarCrawler ecodematch : ecode exist {row[0]}')
    #             if row[0][0] == '0':
    #                 ecode = row[0]
    #             else:
    #                 ecode = '0' + row[0]
    #         item['ecode'] = ecode
    #         list.append(item)
    #
    #     result = pd.DataFrame(list)
    #     result.to_excel("test.xlsx")







