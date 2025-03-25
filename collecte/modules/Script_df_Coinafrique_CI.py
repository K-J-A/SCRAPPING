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

def df_coinafrique_CI():
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
        driver.set_page_load_timeout(5)
        attempts = i
        while attempts > 0:
            try:
                driver.get(start_url)
                driver.maximize_window()
                attempts = 0
            except TimeoutException:
                attempts = attempts - 1
        return driver

    driver = file_config(start_url=r'https://www.google.com')

    
    # Les différentes variables du format de collecte

    Dates = []
    Code_site = []
    Noms = []
    Prix = []
    Superficie = []
    Quartier = []
    Nombre_pieces = []
    Type_immobilier = []
    Commune = []

    def scrape_data(liste_recherche):

        def scrap_page():
            

            Articles_content = driver.find_elements(By.CSS_SELECTOR, "div.card-content.ad__card-content")
            names = []
            links = []
            for Article_content in Articles_content:
                names.append(Article_content.find_element(By.CSS_SELECTOR, 'p.ad__card-description').text)
                Articles_link = Article_content.find_element(By.CSS_SELECTOR, 'p.ad__card-description > a')
                links.append(Articles_link.get_attribute('href'))

            for link in links:
                superficie = None
                nb_piece = None
                type_immo = types
                commune = None
                quartier = None
                prix = None
                name = names[links.index(link)]
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

                    quartier = driver.find_element(By.CSS_SELECTOR, 'div.ad__info__box.ad__info__box-descriptions').text
                    quartier = quartier.replace('\t', '').replace('\n', '')

                    addresses = driver.find_element(By.CSS_SELECTOR, 'p.extras').find_elements(By.CSS_SELECTOR, 'span')
                    for addresse in addresses:
                        if addresse.text.lower().find('abidjan') > -1:
                            commune = addresse.text.lower().replace('\t', '').replace('\n', '')

                    prix = driver.find_element(By.CSS_SELECTOR, 'p.price').text
                    prix = prix.lower().replace('cfa', '').replace('\t', '').replace('\n', '').replace(' ', '')

                    descriptions = driver.find_element(By.CSS_SELECTOR, 'div.details-characteristics').find_elements(By.CSS_SELECTOR, 'li.center')
                    for description in descriptions:
                        if description.text.lower().find('pièce') > -1:
                            nb_piece = description.find_element(By.CSS_SELECTOR, 'span.qt').text

                        if description.text.lower().find('superficie') > -1:
                            superficie = description.find_element(By.CSS_SELECTOR, 'span.qt').text.replace(' m2', '')

                except:
                    continue

                Dates.append(datetime.now().strftime('%y-%m-%d %H:%M:%S'))
                Code_site.append("Coinafrique_CI")
                Noms.append(name)
                Prix.append(prix)
                Superficie.append(superficie)
                Quartier.append(quartier)
                Nombre_pieces.append(nb_piece)
                Type_immobilier.append(type_immo)
                Commune.append(commune)

        for search, types in list(zip(*liste_recherche)):
            url_link = f'https://ci.coinafrique.com/{search}'
            attempts = 2
            while attempts > 0:
                try:
                    driver.get(url_link)
                    driver.maximize_window()
                    attempts = 0
                except TimeoutException:
                    attempts = attempts - 1

            try:
                Paginations = len(driver.find_element(By.CSS_SELECTOR, '.pagination-number').find_elements(By.CSS_SELECTOR, 'span'))
            except:
                Paginations = 0
            scrap_page()

            try:
                for j in range(Paginations - 1):
                    attempts = 2
                    while attempts > 0:
                        try:
                            driver.get(f'{url_link}&page={j + 2}')
                            driver.maximize_window()
                            attempts = 0
                        except TimeoutException:
                            attempts = attempts - 1
                    scrap_page()
            except:
                continue

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

    # Configurer la liste de recherches
    liste_recherche = [
        ["search?sort_by=last&address=Abidjan&category=48&re_offer_type=rent", "search?sort_by=last&address=abidjan&category=51&re_offer_type=rent", "search?sort_by=last&address=abidjan&category=206&re_offer_type=rent", "search?sort_by=last&address=abidjan&category=205&re_offer_type=rent", "search?sort_by=last&address=abidjan&category=254&re_offer_type=rent"],
        ["Villa", "Appartements", "Maisons de vaccance", "Chambre", "Appartements meublés"]
    ]
    df_coinafrique_CI = scrape_data(liste_recherche)
    return df_coinafrique_CI


