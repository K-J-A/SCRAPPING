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
import sys
import random as ran
from playwright.sync_api import sync_playwright
import re


def df_immobilier_CI():
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

    driver = file_config(start_url=r'https://annonces-immobilieres-cote-ivoire.com/recherche-avancee/?status=a-louer&state=abidjan')

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
            Articles_content = page.findAll('div', {'class': 'property-inner'})

            for i in range(len(Articles_content)):

                name = Articles_content[i].find('h2', {'class': 'property-title'}).get_text().strip()

                # Lien de l'article
                Articles_link = Articles_content[i].find('h2', {'class': 'property-title'}).find('a')
                driver.get(Articles_link['href'])
                content = BeautifulSoup(driver.page_source, 'lxml')  # Contenu de la page
                superficie = None
                nb_piece = None
                type_immo = None
                commune = None
                quartier = None
                prix = None

                try:
                    addresses = content.findAll('div', {'class': 'd-flex ere__property-location-item'})
                    for addresse in addresses:
                        if addresse.get_text().strip().lower().find('neighborhood') > -1:
                            quartier = addresse.find('span').get_text().strip()
                        if addresse.get_text().strip().lower().find('town') > -1:
                            commune = addresse.find('span').get_text().strip()

                    descriptions = content.find('ul',{'class': 'ere__list-2-col ere__list-bg-gray'}).findAll('li')
                    for description in descriptions:
                        if description.get_text().strip().lower().find('size') > -1:
                            superficie = description.find('span').get_text().strip()
                            superficie = superficie.replace(' ', '').replace('.', '')[:-2]
                        if description.get_text().strip().lower().find('room') > -1:
                            nb_piece = description.find('span').get_text().strip()
                        if description.get_text().strip().lower().find('type') > -1:
                            type_immo = description.find('span').get_text().strip()
                        if description.get_text().strip().lower().find('price') > -1:
                            prix = description.find('span').get_text().strip()
                            prix = prix.lower().replace(' ', '').replace('\n', '').replace('\t', '').replace('.', '').replace('fcfa', '')
                            prix = prix.replace('.', '').replace('fcfa', '').replace('mille', '')

                except:
                    continue

                Dates.append(datetime.now().strftime('%y-%m-%d %H:%M:%S'))
                Code_site.append("Immobilier_CI")
                Noms.append(name)
                Prix.append(prix)
                Superficie.append(superficie)
                Quartier.append(quartier)
                Nombre_pieces.append(nb_piece)
                Type_immobilier.append(type_immo)
                Commune.append(commune)

        url_link = r'https://annonces-immobilieres-cote-ivoire.com/recherche-avancee/?status=a-louer&state=abidjan'
        driver.get(url_link)
        driver.refresh()
        page_content = BeautifulSoup(driver.page_source, 'lxml')  # Contenu de la page
        scrap_page(page_content)

        try:  # Pour avoir le nombre de page à explorer
            Paginations = int(page_content.find('div', {'class': 'paging-navigation clearfix'}).findAll('a')[-2].get_text().strip())
            for j in range(Paginations - 1):
                driver.get(f'https://annonces-immobilieres-cote-ivoire.com/recherche-avancee/page/{j + 2}/?status=a-louer&state=abidjan')
                driver.refresh()
                page_content = BeautifulSoup(driver.page_source, 'lxml')  # Contenu de la page
                scrap_page(page_content)
        except:
            ''

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

    df_immobilier_CI = scrape_data()
    return df_immobilier_CI



