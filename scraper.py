from selenium import webdriver

from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

import time
import json

class Scraper:

    def __init__(self):
        with open('inputs/indexes.json','r',encoding='utf-8') as f:
            self.indexes = json.load(f)
        with open('inputs/exogenous_indexes.json','r',encoding='utf-8') as f:
            self.exo_indexes = json.load(f)
        
        self.idxs = self.indexes.copy()
        self.idxs.update(self.exo_indexes)    
    
    
    def scrap_zero(self, path, main_url):

        WINDOW_SIZE = "1920,1080"
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=%s" % WINDOW_SIZE)
        driver = webdriver.Chrome(path, options=chrome_options)
        #driver = webdriver.Chrome(path)
        
        driver.get(main_url)
        time.sleep(1)

        delay = 10

        idxs_names = dict()
        historical_data_dict = {}
        for key,value in self.idxs.items(): 
            
            #print(value)
            search_box = WebDriverWait(driver,delay).until(EC.presence_of_element_located((By.XPATH,"//input[@id='yfin-usr-qry']")))
            search_box.send_keys(value)
            time.sleep(1)
            search_button = WebDriverWait(driver,delay).until(EC.presence_of_element_located((By.XPATH,"//button[@id='header-desktop-search-button']")))
            search_button.click()
            time.sleep(0.5)
            tmp_name = WebDriverWait(driver,delay).until(EC.presence_of_element_located((By.XPATH,"//div[@class='D(ib) ']/h1[1]"))).get_attribute('innerHTML')
            idxs_names[key] = tmp_name
            
            
            #se va a por los datos historicos
            historical_data = WebDriverWait(driver,delay).until(EC.presence_of_element_located((By.XPATH,'//li[@data-test="HISTORICAL_DATA"]')))
            historical_data.click()
            time.sleep(2.5)
            
            time_period = WebDriverWait(driver,delay).until(
                EC.presence_of_element_located(
                    (By.XPATH,'//div[contains(@class,"Pos(r) D(ib) Va(m) Mstart")]')
                )
            )
            time_period.click()
            time.sleep(2.5)
            
            
            historical_data = WebDriverWait(driver,delay).until(
                EC.presence_of_element_located(
                    (By.XPATH,'//table[@data-test="historical-prices"]')
                )
            )
            
            hd_list = historical_data.find_elements_by_xpath('//tbody/tr')
            dates = []
            prices = []
            for element in hd_list:
                try:
                    date  = element.find_element_by_xpath('./td[1]/span').get_attribute('innerHTML')
                except:
                    date = None
                try:
                    price = element.find_element_by_xpath('./td[5]/span').get_attribute('innerHTML')
                except:
                    price = None
                dates.append(date)
                prices.append(price)
            
            historical_data_dict[key] = {dates[i]:prices[i] for i in range(len(dates))}
            
            with open("outputs/{}_close_prices.json".format(key), "w") as outfile:
                json.dump(historical_data_dict[key], outfile)
            
            with open("outputs/idxs_names.json", "w") as outfile:
                json.dump(idxs_names, outfile)
    
    def scrap_reg(self, path, main_url):

        WINDOW_SIZE = "1920,1080"
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=%s" % WINDOW_SIZE)
        driver = webdriver.Chrome(path, options=chrome_options)
        #driver = webdriver.Chrome(path)
        
        driver.get(main_url)
        time.sleep(1)

        delay = 10

        ultimate_data = {}
        for key,value in self.idxs.items():
            search_box = WebDriverWait(driver,delay).until(EC.presence_of_element_located((By.XPATH,"//input[@id='yfin-usr-qry']")))
            search_box.send_keys(value)
            time.sleep(1)
            search_button = WebDriverWait(driver,delay).until(EC.presence_of_element_located((By.XPATH,"//button[@id='header-desktop-search-button']")))
            search_button.click()
            time.sleep(0.5)
            
            #se va a por los datos historicos
            historical_data = WebDriverWait(driver,delay).until(EC.presence_of_element_located((By.XPATH,'//li[@data-test="HISTORICAL_DATA"]')))
            historical_data.click()
            time.sleep(2.5)

            try:
                last_close_value = WebDriverWait(driver,delay).until(EC.presence_of_element_located(
                    (By.XPATH,'//table[@data-test="historical-prices"]/tbody/tr[1]/td[5]/span'))
                    ).get_attribute('innerHTML')
            except:
                last_close_value = None

            
            try:
                last_close_value_date = WebDriverWait(driver,delay).until(EC.presence_of_element_located(
                    (By.XPATH,'//table[@data-test="historical-prices"]/tbody/tr[1]/td[1]/span'))
                    ).get_attribute('innerHTML')
            except:
                last_close_value_date = None
            
            ultimate_data[key] = {last_close_value_date : last_close_value}

        with open("outputs/ultimate_data.json", "w") as outfile:
                json.dump(ultimate_data, outfile)    

            
            


        