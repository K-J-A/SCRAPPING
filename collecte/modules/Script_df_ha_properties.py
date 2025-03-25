import pandas as pd
import requests
import statistics as stat
import lxml
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import os
from datetime import datetime
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
import sys
import random as ran
from playwright.sync_api import sync_playwright
import re
import time


def df_ha_properties():
    def file_config(start_url, i=2):
        # Configuration du driver
        driver_path = r'/usr/bin/chromedriver'

        # Configurer les options Chrome
        options = Options()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument("--disable-notifications")  # Désactive les notifications
        #options.add_argument("--headless")  # Optionnel : pour exécuter Chrome en mode sans tête (invisible)

        # Initialiser le service et le WebDriver
        service = Service(driver_path)
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(6000)
        attempts = i
        while attempts > 0:
            try:
                driver.get(start_url)
                driver.maximize_window()
                attempts = 0
            except TimeoutException:
                attempts = attempts - 1
        return driver

    driver = file_config(start_url=r'https://www.ha-properties.com/location?prod.prod_type=')

    wait = WebDriverWait(driver, 10000)

    # Les différentes variables du format de collecte

    Dates = []
    Code_site = []
    Noms = []
    Prix = []
    Superficie = []
    Quartier =[]
    Nombre_pieces = []
    Type_immobilier = []
    Commune = []


    def scrape_data():

        def scrap_page():
            WebDriverWait(driver, 30).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "._ycyc75.theme1"))
            )
            Articles_content = driver.find_elements(By.CSS_SELECTOR, "._ycyc75.theme1")
            for Article_content in Articles_content:
                superficie = None
                nb_piece = None
                type_immo = "RAS"
                commune = "RAS"
                quartier = None
                prix = Article_content.find_element(By.CSS_SELECTOR, 'span._1s9k5jx.undefined.textblock ').text.lower()
                prix = prix.replace(' ', '').replace('\t', '').replace('\n', '').replace('fcfa/moishc', '')

                name = Article_content.find_element(By.CSS_SELECTOR, 'h2._2z4488._1o5lotl.textblock ').text

                try:

                    quartier = Article_content.find_element(By.CSS_SELECTOR, 'p._1176jqw._1jn5w0e.textblock ').text
                    quartier = quartier.replace('\t', '').replace('\n', '')

                    descriptions = Article_content.find_elements(By.CSS_SELECTOR, 'span._7navjo > span._15b2ryg')
                    for description in descriptions:

                        if description.text.lower().find('m²') > -1:
                            superficie = description.text.lower().replace('\t', '').replace('\n', '')
                            superficie = superficie.replace('m²', '').replace('.', ',').replace(' ', '')

                        if description.text.lower().find('pièce') > -1:
                            nb_piece = description.text.lower().replace('pièce', '').replace('s', '').replace('\t', '').replace('\n', '')
                            nb_piece = int(nb_piece.replace(' ', ''))

                except:
                    continue

                Dates.append(datetime.now().strftime('%y-%m-%d %H:%M:%S'))
                Code_site.append("HA-properties")
                Noms.append(name)
                Prix.append(prix)
                Superficie.append(superficie)
                Quartier.append(quartier)
                Nombre_pieces.append(nb_piece)
                Type_immobilier.append(type_immo)
                Commune.append(commune)



        url_link = r'https://www.ha-properties.com/location?prod.prod_type='
        driver.get(url_link)
        driver.refresh()
        scrap_page()
        i = 2
        while True:
            try:
                WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, "._1bnecuy._wnpftc._16r83l9.false"))
                )
                driver.get(fr'https://www.ha-properties.com/location?page={i}')
                driver.refresh()
                scrap_page()
                i = i + 1
                """element = driver.find_element(By.CSS_SELECTOR, "._1bnecuy._wnpftc._16r83l9.false")
                actions = ActionChains(driver)
                actions.move_to_element(element).click().perform()
                time.sleep(10)"""
            except TimeoutException:
                break

        # Les données resultant du scriping

        data = {
            'Date_de_collecte': Dates,
            'Code_site': Code_site,
            'Libelle_du_produit': Noms,
            'Prix_du_produit': Prix,
            'Superficie': Superficie,
            'Quartier': Quartier,
            'Nombre_pieces': Nombre_pieces,
            'Type_immobilier': Type_immobilier,
            'Commune': Commune

            }
        data = pd.DataFrame.from_dict(data)

        return data

    df_ha_properties = scrape_data()
    return df_ha_properties



