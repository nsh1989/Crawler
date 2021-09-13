# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from Crawler.encarCrawler import CrawlerEncar
from Database.dbmanager import DBManager

# def print_hi(name):
#     # Use a breakpoint in the code line below to debug your script.
#     print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.
#

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    dbManager = DBManager()
    encarCrawler = CrawlerEncar()
    encarCrawler.run()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
