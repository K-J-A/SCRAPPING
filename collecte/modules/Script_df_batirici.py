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


def df_batirici():
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

    driver = file_config(start_url=r'https://batirici-immobilier.com/location/')

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

        def scrap_page(page):
            WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".col-lg-4.col-12.col-sm-6.col-md-6.listing_wrapper"))
            )
            Articles_content = driver.find_elements(By.CSS_SELECTOR, ".col-lg-4.col-12.col-sm-6.col-md-6.listing_wrapper")
            names = []
            links = []
            for Article_content in Articles_content:
                names.append(Article_content.find_element(By.CSS_SELECTOR, 'div.property-unit-information-wrapper > h4').text)
                Articles_link = Article_content.find_element(By.CSS_SELECTOR,'div.property-unit-information-wrapper > h4 > a')
                links.append(Articles_link.get_attribute('href'))

            for link in links:
                superficie = None
                nb_piece = None
                type_immo = None
                commune = None
                quartier = None
                prix = None
                name = names[links.index(link)]
                # Lien de l'article
                driver.get(link)
                wait = WebDriverWait(driver, 10000)

                try:
                    type_immo = driver.find_element(By.CSS_SELECTOR,'div.status-wrapper.verticalstatus > div').text
                    addresses = driver.find_element(By.CSS_SELECTOR,'#accordion_property_address_collapse').find_elements(By.CSS_SELECTOR,'div.row > div')
                    for addresse in addresses:
                        if addresse.text.lower().find('address') > -1:
                            quartier = addresse.text.lower().replace('address:', '').replace('\t', '').replace('\n', '')

                        if addresse.text.lower().find('city') > -1:
                            commune = addresse.text.lower().replace('city:', '').replace('\t', '').replace('\n', '')

                    descriptions = driver.find_element(By.CSS_SELECTOR, '#accordion_property_details_collapse').find_elements(By.CSS_SELECTOR, 'div.row > div')
                    for description in descriptions:

                        if description.text.lower().find('lot size') > -1:
                            superficie = description.text.lower().replace('property lot size:', '').replace('\t', '').replace('\n', '')
                            superficie = superficie.replace('m²', '').replace('.', ',')[:-2]
                        if description.text.lower().find('room') == 0:
                            nb_piece = description.text.lower().replace('rooms:', '').replace('\t', '').replace('\n', '')
                        if description.text.lower().find('price') > -1:
                            prix = description.text.lower().replace('price:', '').replace('\t', '').replace('\n', '')
                            prix = prix.replace(' ', '').replace('.', '').replace('fcfa', '').replace('parmois', '')
                except:
                    continue

                Dates.append(datetime.now().strftime('%y-%m-%d %H:%M:%S'))
                Code_site.append("Batirici")
                Noms.append(name)
                Prix.append(prix)
                Superficie.append(superficie)
                Quartier.append(quartier)
                Nombre_pieces.append(nb_piece)
                Type_immobilier.append(type_immo)
                Commune.append(commune)



        url_link = r'https://batirici-immobilier.com/location/'
        driver.get(url_link)
        driver.refresh()
        page_content = BeautifulSoup(driver.page_source, 'lxml')  # Contenu de la page
        # clicker sur le bouton decouvrir
        while True:
            try:
                WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, ".wpresidence_button.wpestate_item_list_sh"))
                )
                element = driver.find_element(By.CSS_SELECTOR, ".wpresidence_button.wpestate_item_list_sh")
                actions = ActionChains(driver)
                actions.move_to_element(element).click().perform()
                time.sleep(10)
            except TimeoutException:
                break

        scrap_page(page_content)

        """try:  # Pour avoir le nombre de page à explorer
            Paginations = int(page_content.find('div', {'class': 'paging-navigation clearfix'}).findAll('a')[-2].get_text().strip())
            for j in range(Paginations - 1):
                driver.get(f'https://annonces-immobilieres-cote-ivoire.com/recherche-avancee/page/{j + 2}/?status=a-louer&state=abidjan')
                driver.refresh()
                page_content = BeautifulSoup(driver.page_source, 'lxml')  # Contenu de la page
                scrap_page(page_content)
        except:
            ''"""

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

    df_batirici = scrape_data()
    return df_batirici


