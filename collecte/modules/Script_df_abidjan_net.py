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


def df_abidjan_net():
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

    driver = file_config(start_url=r'https://annonces.abidjan.net/immobiliers?search_immo_form%5BidTypeImmobilier%5D=&search_immo_form%5Bprix%5D=&search_immo_form%5Bsuperficie%5D=&search_immo_form%5Bstatut%5D=Location&search_immo_form%5Bville%5D=')

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
            Articles_content = page.findAll('div', {'class': 'annonce anim'})

            for i in range(len(Articles_content)):

                name = Articles_content[i].find('div', {'class': 'annonce-description'}).get_text().strip()

                # Lien de l'article
                Articles_link = Articles_content[i].find('a')
                driver.get("https://annonces.abidjan.net" + Articles_link['href'])
                content = BeautifulSoup(driver.page_source, 'lxml')  # Contenu de la page
                superficie = None
                nb_piece = None
                type_immo = None
                commune = None
                quartier = None

                try:
                    localisation = content.find('div', {'class': 'annonce-details-country'}).get_text().strip()
                    prix = content.find('div', {'class': 'annonce-details-price'}).get_text().strip()
                    prix = prix.replace('\t', '').replace('\n', '').replace(' ', '')
                    if localisation.lower().find('abidjan') > -1:
                        descriptions = content.findAll('div',{'class': 'annonce-details-p'})
                        for description in descriptions:
                            if description.get_text().strip().lower().find('superficie') > -1:
                                superficie = description.find('strong').get_text().strip()
                            if description.get_text().strip().lower().find('nombre') > -1:
                                nb_piece = description.find('strong').get_text().strip()
                            if description.get_text().strip().lower().find('type') > -1:
                                type_immo = description.find('strong').get_text().strip()
                            if description.get_text().strip().lower().find('commune') > -1:
                                commune = description.find('strong').get_text().strip()
                            if description.get_text().strip().lower().find('quartier') > -1:
                                quartier = description.find('strong').get_text().strip()

                except:
                    continue

                Dates.append(datetime.now().strftime('%y-%m-%d %H:%M:%S'))
                Code_site.append("Abidjan_net")
                Noms.append(name)
                Prix.append(prix[prix.find(':') + 1:-3])
                Superficie.append(superficie)
                Quartier.append(quartier)
                Nombre_pieces.append(nb_piece)
                Type_immobilier.append(type_immo)
                Commune.append(commune)

        url_link = r'https://annonces.abidjan.net/immobiliers?search_immo_form%5BidTypeImmobilier%5D=&search_immo_form%5Bprix%5D=&search_immo_form%5Bsuperficie%5D=&search_immo_form%5Bstatut%5D=Location&search_immo_form%5Bville%5D='
        driver.get(url_link)
        driver.refresh()
        page_content = BeautifulSoup(driver.page_source, 'lxml')  # Contenu de la page
        scrap_page(page_content)
        try:  # Pour avoir le nombre de page à explorer
            Paginations = int(page_content.findAll('li', {'class': 'page-item'})[-2].find('a').get_text().strip())
            for j in range(Paginations - 1):
                driver.get(f'{url_link}&page={j + 2}')
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

    df_abidjan_net = scrape_data()
    return df_abidjan_net


