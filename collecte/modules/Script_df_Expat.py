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


def df_expat():
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
        driver.set_page_load_timeout(10)
        attempts = i
        while attempts > 0:
            try:
                driver.get(start_url)
                driver.maximize_window()
                attempts = 0
            except TimeoutException:
                attempts = attempts - 1
        return driver

    driver = file_config(start_url=r'https://www.expat.com/fr/immobilier/rechercheresultat/cHJpbWFyeS1maWx0ZXJ8MnwyfGZyfDE1MHwxMTYzMHwxMTE1MzE1MXxBYmlkamFuLCBDw7R0ZSBkJiMzOTtJdm9pcmV8MXwyfEFiaWRqYW4sIEPDtHRlIGQmIzM5O0l2b2lyZXw5fHx8fA--/')

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
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".classified-wrapper "))
            )
            Articles_content = driver.find_elements(By.CSS_SELECTOR, ".classified-wrapper ")
            names = []
            links = []
            for Article_content in Articles_content:
                names.append(Article_content.find_element(By.CSS_SELECTOR, 'a.list-link').text)
                Articles_link = Article_content.find_element(By.CSS_SELECTOR,'a.list-link')
                links.append(Articles_link.get_attribute('href'))

            for link in links:
                superficie = None
                nb_piece = None
                type_immo = "RAS"
                commune = None
                quartier = None
                prix = None
                #name = names[links.index(link)]
                # Lien de l'article
                attempts = 2
                while attempts > 0:
                    try:
                        driver.get(link)
                        driver.maximize_window()
                        attempts = 0
                    except TimeoutException:
                        attempts = attempts - 1

                try:
                    name = driver.find_element(By.CSS_SELECTOR, 'div.classified-ads-header-container > h1').text
                    name = name.replace('\t', '').replace('\n', '')

                    

                    quartier = driver.find_element(By.CSS_SELECTOR, '.housing-text').text
                    quartier = quartier.replace('\t', '').replace('\n', '')

                    descriptions = driver.find_elements(By.CSS_SELECTOR, 'div.item-attributes-container > p > span.value')
                    for description in descriptions:
                        if description.text.lower().find('abidjan') > -1:
                            commune = description.text.lower().replace('\t', '').replace('\n', '')

                        if description.text.lower().find('surface') > -1:
                            superficie = description.text.lower().replace('surface habitable', '').replace('\t', '').replace('\n', '')
                            superficie = superficie.replace('m2', '').replace('.', ',')

                        if description.text.lower().find('chambre') > -1:
                            nb_piece = description.text.lower().replace(' chambres', '').replace('\t', '').replace('\n', '')
                            nb_piece = int(nb_piece) + 1
                        if description.text.lower().find('fr') > -1:
                            prix = description.text.lower().replace('fr', '').replace('\t', '').replace('\n', '')
                            prix = prix.replace(' ', '').replace(',', '').replace('/parmois', '')

                except:
                    continue

                Dates.append(datetime.now().strftime('%y-%m-%d %H:%M:%S'))
                Code_site.append("Expat")
                Noms.append(name)
                Prix.append(prix)
                Superficie.append(superficie)
                Quartier.append(quartier)
                Nombre_pieces.append(nb_piece)
                Type_immobilier.append(type_immo)
                Commune.append(commune)



        url_link = r'https://www.expat.com/fr/immobilier/rechercheresultat/cHJpbWFyeS1maWx0ZXJ8MnwyfGZyfDE1MHwxMTYzMHwxMTE1MzE1MXxBYmlkamFuLCBDw7R0ZSBkJiMzOTtJdm9pcmV8MXwyfEFiaWRqYW4sIEPDtHRlIGQmIzM5O0l2b2lyZXw5fHx8fA--/'
        #driver.get(url_link)
        #driver.refresh()
        # clicker sur le bouton voir plus
        while True:
            try:
                WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, ".classified-main-wrapper__btn"))
                )
                element = driver.find_element(By.CSS_SELECTOR, ".classified-main-wrapper__btn")
                actions = ActionChains(driver)
                actions.move_to_element(element).click().perform()
                time.sleep(10)
            except TimeoutException:
                break

        scrap_page()

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

    df_expat = scrape_data()
    return df_expat


