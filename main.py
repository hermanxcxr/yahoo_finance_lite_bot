from scraper import Scraper
from db_connector import DbConnector
from models import Models
from regular_execution import RegularExecution

if __name__ == '__main__':

    scraper = Scraper()
    db_connector = DbConnector()
    
    condition = input('Desea realizar una predicción) (y/n): ')

    if condition == '0':
        scraper.scrap_zero('../../../selenium_driver/chromedriver96.exe','https://finance.yahoo.com/')
        db_connector.connector_zero()
        models = Models()
        models.training()

    
    if condition == 'y':
        scraper.scrap_reg('../../../selenium_driver/chromedriver96.exe','https://finance.yahoo.com/')
        db_connector.connector_reg()
        regular_execution = RegularExecution()
        regular_execution.forecast()