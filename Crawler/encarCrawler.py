import datetime
import dateutil.parser
import sys
import pandas as pd
import pymysql.cursors

from Crawler.baseCrawler import CrawlerBase
from Crawler.baseCrawler import html
from Utils.datAPI import cdatapi


class CrawlerEncar(CrawlerBase):
    totalCount = 0
    dat = cdatapi()
    def make_sr(self, num):
        sr_param = "|ModifiedDate|" + str(num) + "|100"
        return sr_param

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

    def orgCarID(self, photopath):
        ID = str(photopath).split('/')
        ID = ID[len(ID)-1]
        ID = ID.replace('_', '')
        return ID

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

    def getlist(self):
        pages = self.totalCount / 100
        if self.totalCount % 100 > 0:
            pages = pages + 1
        for page in range(0, int(pages)):
            self.param['sr'] = self.make_sr(page * 100)
            resp = self.s.get(self.url, params=self.param).json()['SearchResults']
            for item in resp:
                dict = {}
                if self.checkID(item['Id']) :
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
                dict['YearMon'] = item['Year']
                dict['Mileage'] = item['Mileage']
                dict['Price'] = item['Price']
                # dict['Photo'] = ''
                # try:
                #     dict['Photo'] = item['Photo']
                # except Exception as e:
                #     print(f"Photo : {e}")
                #     continue
                #     pass
                # dict['ServiceCopyCar'] = item['ServiceCopyCar']
                # dateFormatter = "%Y-%m-%d %H:%M:%S.%f"
                # dict['ModifiedDate'] = self.strToDate(item['ModifiedDate'])
                # # dict['ModifiedDate'] = datetime.datetime.strptime(item['ModifiedDate'], dateFormatter)
                # dict['updatedDate'] = ''
                # try:
                #     dict['updatedDate'] = self.strToDate(item['Photos'][0]['updatedDate'])
                #     print(dict['updatedDate'])
                # except Exception as e:
                #     print(f"updatedDate : {e}")
                #     pass
                # try:
                #     dict['orgID'] = self.orgCarID(dict['Photo'])
                # except Exception as e:
                #     print(f"Photo orgID: {e}")
                #     pass
                self.insertTable(dict, 'encarlist')
            print(page)

    def getCarInfo(self, id):
        dict = {}
        url = 'http://www.encar.com/md/sl/mdsl_regcar.do?method=inspectionViewNew&carid=' + str(id)
        # resp = s.get(url)
        dict['org_carid'] = id
        dict['vin'] = ''
        dict['accident'] = ''
        dict['repair'] = ''
        try:
            resp = self.s.get(url, allow_redirects=False)
        except Exception as e:
            print(e)

        if resp.status_code == 200:
            content = resp.content
            tree = html.fromstring(content)
            try:
                vin = tree.xpath('//*[@id="bodydiv"]/div[2]/div/div[2]/table/tbody/tr[4]/td[2]')
                dict['vin'] = vin[0].text
                print(vin[0].text)
            except Exception as e:
                print(e)
                pass
            trs = tree.xpath('//table[@class="tbl_repair"]/tbody/tr')
            if len(trs) <= 0:
                print('200 len error')
                return dict
            else:
                # 사고이력
                try:
                    spans = trs[0].xpath('.//span')
                    if spans[0].get('class') == 'txt_state on':
                        dict['accident'] = True
                    else:
                        dict['accident'] = False

                    spans = trs[1].xpath('.//span')
                    if spans[0].get('class') == 'txt_state on':
                        dict['repair'] = True
                    else:
                        dict['repair'] = False
                except Exception as e:
                    print(e)

    def getSellerInfo(self, id):
        pass

    def run(self):
        self.getlist()

    def getDetail(self, id):
        pass

    def getSoldDT(self, id):
        pass

    def ChangeExistance(self, id):
        sql = "UPDATE encarcheklist SET CheckDetail = 1, Existance = 0 where encarID = %s " % (id)
        self.dbManager.cursor.execute(sql)

    def detailProcess(self):
        sql = "SELECT encarID From encarcheklist WHERE CheckDetail = 0 and Existance = 1 ;"
        print(sql)
        self.dbManager.cursor.execute(sql)
        rows = [item[0] for item in self.dbManager.cursor.fetchall()]
        print(rows[0])
        url = f'http://www.encar.com/dc/dc_cardetailview.do'
        req = {
            'method': 'ajaxInspectView',
            'sdFlag': 'N',
            'rgsid': ''
        }
        # soldDt 확인 코드
        # test = '27787073'
        # req['rgsid'] = test
        # resp = self.s.get(url, params=req)
        # data = resp.json()[0]
        # print(data)
        # date = data['inspect']['carSaleDto']['soldDt']['time']
        # print(date)
        # print(self.strToDate(str(date)))
        for row in rows:
            req['rgsid'] = row
            resp = self.s.get(url, params=req)
            data = resp.json()[0]
            if data['msg'] == 'FAIL':
                self.ChangeExistance(row)
                continue
            data = data['inspect']
            uploadData = {}
            #URL
            uploadData['URL'] = resp.url
            #매물최초등록일
            try:
                year = data['carSaleDto']['firstRegDt']['year']
                year = year - 100 + 2000
                month = data['carSaleDto']['firstRegDt']['month']
                month = int(month) + 1
                date = data['carSaleDto']['firstRegDt']['date']
                Date = str(year) + "-" + str(month) + "-" + str(date)
                uploadData['UPDATEDDATE'] = self.strToDate(Date)
            except Exception as e:
                time = data['carSaleDto']['firstRegDt']
                print(row)
                print(pd.to_datetime(time, unit='ms'))
                print(type(pd.to_datetime(time, unit='ms')))
                uploadData['UPDATEDDATE'] = self.strToDate(Date)
            #매물변경일
            try:
                year = data['carSaleDto']['mdfDt']['year']
                year = int(year) - 100 + 2000
                month = data['carSaleDto']['mdfDt']['month']
                month = int(month) + 1
                date = data['carSaleDto']['mdfDt']['date']
                Date = str(year) + "-" + str(month) + "-" + str(date)
                uploadData['ModifiedDate'] = self.strToDate(Date)
            except Exception as e:
                print(e)
            # 사고이력
            try:
                uploadData['ACCIDENT'] = data['direct']['inspectAccidentSummary']['accident']['code']
            except Exception as e:
                print(e)
            # 수리이력
            try:
                uploadData['REPAIR'] = data['direct']['inspectAccidentSummary']['simpleRepair']['code']
            except Exception as e:
                print(e)
            # vin
            try:
                if len(data['direct']['master']['carregiStration']) == 17:
                    uploadData['VIN'] = data['direct']['master']['carregiStration']
            except Exception as e:
                print(e)
            self.updateTable(uploadData, 'encarlist', row)
            self.ChangeExistance(row)

    def ecodesearch(self, vin):
        ecode = self.dat.getEcodeByVin(vin)
        return ecode

    def test(self):

        sql = """SELECT *, COUNT(*)
        FROM encarlist WHERE VIN is not null GROUP BY MANUFACTURER, MODEL, BADGE, BADGEDETAIL, FUELTYPE, TRANSMISSION, FORMYEAR
        HAVING COUNT(*) > 0;"""
        cursor = self.dbManager.conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(sql)
        dict = cursor.fetchall()

        # columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in dict.keys())
        # values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in dict.values())
        # set = """,""".join("""{}='{}'""".format(k, v) for k, v in dict.items())
        # sql = "UPDATE  %s SET %s WHERE ID = %s;" % (table, set, id)
        # print(sql)
        # self.dbManager.cursor.execute(sql)
        # self.dbManager.conn.commit()
        dat = cdatapi()

        for item in dict:
            src = {}
            src['badge'] = item['BADGE']
            src['badgedetail'] = item['BADGEDETAIL']
            src['manufacturer'] = item['MANUFACTURER']
            src['model'] = item['MODEL']
            src['fueltype'] = item['FUELTYPE']
            src['transmission'] = item['TRANSMISSION']
            src['formyear'] = item['FORMYEAR']
            where = """and """.join("""{}='{}'""".format(k, v) for k, v in src.items())

            sql = """
            SELECT COUNT(*) FROM encarecode
            WHERE %s HAVING COUNT(*) > 0;
            """ % (where)
            self.dbManager.cursor.execute(sql)
            row = self.dbManager.cursor.fetchall()

            if len(row) > 0:
                print('encarCrawler : ecode exist')
                continue

            ecode = self.ecodesearch(item['VIN'])
            if ecode is None:
                print("encarCrawler : ecode : None")
                continue
            print(ecode)
            src['ecode'] = ecode
            self.insertTable(src, 'encarecode')
            print(f'encarCrawler : {ecode} inserted')

    def ecodematch(self):

        sql = """
        SELECT * FROM encarlist;
        """
        cursor = self.dbManager.conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(sql)
        dict = cursor.fetchall()
        list = []
        for item in dict:
            src = {}
            src['badge'] = item['BADGE'].replace("'", "''")
            src['badgedetail'] = item['BADGEDETAIL'].replace("'", "''")
            src['manufacturer'] = item['MANUFACTURER']
            src['model'] = item['MODEL']
            src['fueltype'] = item['FUELTYPE']
            src['transmission'] = item['TRANSMISSION']
            src['formyear'] = item['FORMYEAR']
            where = """and """.join("""{}='{}'""".format(k, v) for k, v in src.items())

            sql = """
            SELECT ecode, COUNT(*) FROM encarecode
            WHERE %s HAVING COUNT(*) > 0;
            """ % (where)
            self.dbManager.cursor.execute(sql)
            row = [item[0] for item in self.dbManager.cursor.fetchall()]

            ecode = ''
            if len(row) < 1:
                print('encarCrawler ecodematch : ecode not exist')
            else:
                print(f'encarCrawler ecodematch : ecode exist {row[0]}')
                if row[0][0] == '0':
                    ecode = row[0]
                else:
                    ecode = '0' + row[0]
            item['ecode'] = ecode
            list.append(item)

        result = pd.DataFrame(list)
        result.to_excel("test.xlsx")







