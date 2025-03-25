import pandas as pd
import requests
import statistics as stat
import lxml
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import os
from datetime import datetime
from selenium.common.exceptions import TimeoutException
import sys
import re
import random as ran

"""from Script_df_abidjan_net import df_abidjan_net
from Script_df_batirici import df_batirici
from Script_df_coinafrique_CI import df_coinafrique_CI
from Script_df_expat import df_expat
from Script_df_ha_properties import df_ha_properties
from Script_df_immobilier_CI import df_immobilier_CI"""


def preprocessing(data, file):
    # ETAPE 1 -------------------------------------------------------------------------------------------------------------------------------------------------------------

    def superficie_traitement():
        ## Supprimer les données qui n'ont ni superficie ni prix ni nb_piece
        data_clean = data.dropna(subset=["Superficie", "Prix_du_produit", "Nombre_pieces"])
        ## Supprimer les nombre des pièces >= 30
        data_clean = data_clean[data_clean['Nombre_pieces'] < 30]
        # Traitement superficie
        def traitement_superficie(element):
            try:
                element = float(element)
            except:
                element = float(element[0:element.find(".") + 4].strip().replace(".", ""))
            if element > 3000 or element < 12:
                element = None
            return element

        data_clean["Superficie"] = data_clean["Superficie"].apply(lambda row: traitement_superficie(str(row).replace(",", ".")))

        return data_clean


    # ETAPE 2 -------------------------------------------------------------------------------------------------------------------------------------------------------------

    def type_immo_traitement():
        data_clean = superficie_traitement()
        ## Types de logement concernés
        bad_types = ['bureau', 'hôtel', 'hotel', 'commerce', 'cour', 'divers', 'entrepot', 'entrepôt', 'immeuble', 'locaux',
                     'magasin', 'ras', 'terrain']
        data_clean = data_clean[~data_clean['Type_immobilier'].str.contains('|'.join(bad_types), case=False, na=False)]

        # Traitement type immobilier# Traitement type immobilier
        def traitement_type_immo(element):
            if re.search(rf"(?=.*(meuble|meublé|confort|vacance))",  str(element["Libelle_du_produit"]).lower().replace('\n', '')) or re.search(rf"(?=.*(meublé|résidence|vacance))",  str(element["Type_immobilier"]).lower().replace('\n', '')):
                element["Type_immobilier"] = "Résidence meublée"

            elif re.search(rf"(?=.*(duplex|triplex))",  str(element["Libelle_du_produit"]).lower().replace('\n', '')) or re.search(rf"(?=.*(duplex|triplex))",  str(element["Type_immobilier"]).lower().replace('\n', '')):
                element["Type_immobilier"] = "Duplex ou triplex"

            elif re.search(rf"(?=.*(appartement|studio))",  str(element["Type_immobilier"]).lower().replace('\n', '')):
                element["Type_immobilier"] = "Appartement"

            elif re.search(rf"(?=.*(maison|villa))",  str(element["Type_immobilier"]).lower().replace('\n', '')):
                element["Type_immobilier"] = "Maison"
            else:
                element["Type_immobilier"] = None
            return element

        data_clean[["Libelle_du_produit", "Type_immobilier"]] = data_clean[["Libelle_du_produit", "Type_immobilier"]].apply(lambda row: traitement_type_immo(row), axis=1)

        return data_clean



    # ETAPE 3 -------------------------------------------------------------------------------------------------------------------------------------------------------------

    def prix_traitement():
        data_clean = type_immo_traitement()
        # Traitement prix
        def traitement_prix(element):

            if re.sub(r'\D', '', str(element["Prix_du_produit"])) == '':
                element["Prix_du_produit"] = None
            elif element["Type_immobilier"] == "Résidence meublée" and str(element["Prix_du_produit"]).lower().find("jour") > -1:
                element["Prix_du_produit"] = int(re.sub(r'\D', '', str(element["Prix_du_produit"]))) * 30
            elif element["Type_immobilier"] == "Résidence meublée" and int(re.sub(r'\D', '', str(element["Prix_du_produit"]))) in range(5000, 151000):
                element["Prix_du_produit"] = int(re.sub(r'\D', '', str(element["Prix_du_produit"]))) * 30
            elif element["Type_immobilier"] == "Résidence meublée" and int(re.sub(r'\D', '', str(element["Prix_du_produit"]))) in range(151000, 500001) and re.search(rf"(?=.*(parjour|/jour|nuitée))", str(element["Quartier"]).lower().replace('\n', '').replace(' ', '')):
                element["Prix_du_produit"] = int(re.sub(r'\D', '', str(element["Prix_du_produit"]))) * 30
            elif element["Type_immobilier"] == "Résidence meublée" and (int(re.sub(r'\D', '', str(element["Prix_du_produit"]))) < 5000 or int(re.sub(r'\D', '', str(element["Prix_du_produit"]))) > 15000000):
                element["Prix_du_produit"] = None
            elif element["Type_immobilier"] != "Résidence meublée" and (int(re.sub(r'\D', '', str(element["Prix_du_produit"]))) < 30000 or int(re.sub(r'\D', '', str(element["Prix_du_produit"]))) > 7000000):
                element["Prix_du_produit"] = None
            else:
                element["Prix_du_produit"] = int(re.sub(r'\D', '', str(element["Prix_du_produit"])))
            return element

        data_clean[["Prix_du_produit", "Type_immobilier", "Quartier"]] = data_clean[["Prix_du_produit", "Type_immobilier", "Quartier"]].apply(lambda row: traitement_prix(row), axis=1)

        return data_clean

    # ETAPE 4 -------------------------------------------------------------------------------------------------------------------------------------------------------------

    def commune_traitement():
        data_clean = prix_traitement()
        # Traitement quartier et commune
        def traitement_quartier_commune(element):
            commune_to_quartier = {
                # Abobo
                'abobo': [r'sogephia', r'avocatier', r'abobo doum(é|e)', r'pk18', r'anonkoua kout(é|e)', r'avocatier', r'soge(ph|f)ia', r'sog(ph|f)ia',
                          r'belleville', r'derri(è|e)re rails', r'n’dotr(é|e)', r'samak(é|e)', r'anador', r'lyc(é|e)e fran(ç|c)ais', r'abobo sipim 4', r'7(è|e) tranche'],
                # Adjamé
                'adjamé': [r'williamsville', r'bracodi', r'ind(é|e)ni(é|e)', r'220 logements', r'adjam(é|e) village',
                           r'libert(é|e)', r'habitat', r'fraternit(é|e)'],
                # Attécoubé
                'attécoubé': [r'banco', r'abobodoum(é|e)', r'locodjro', r'mossikro', r'agban', r'anonkoua',
                              r'att(é|e)coub(é|e) village', r'sanctuaire marial'],
                # Cocody
                'cocody': [r'cnps', r'rti', r'cocody centre', r'9(è|e) tranche', r'danga', r'angré', r'angre', r'abatta',
                           r'riviera', r'rivera',  r'riviéra', r'rivi(é|e)ra 1', r'rivi(é|e)ra 2', r'rivi(é|e)ra 3', r'rivi(é|e)ra 4',
                           r'riviera4', r'palmeraie', r'deux plateaux', r'2 plateaux', r'ii plateaux', r'danga', r'mermoz',
                           r'vallon', r'cité des arts', r'cité rouge', r'ambassade', r'golf', r'faya', 'bonoumin',
                           'attoban', r'djorogobit(é|e)', r'rosier 5', r'programme 1', r'rue des jardins',
                           'lycée technique', 'riviera triangle', 'abatta', 'école de police', 'ecole de police',
                           'm\'badon', 'riviera4', r'beverly hills', r'canebi(e|è)re', r'abidjan mall', r'boston hills', r'mahou'],
                # Koumassi
                'koumassi': [r'divo', r'remblais', r'prodomo', r'campement', r'sicogi', r'grand campement',
                             r'zone industrielle', r'addoha'],
                # Marcory
                'marcory': [r'marcory résidentiel', r'orca deco', r'zone 4', r'bietry', r'biétry', r'anoumabo', r'hibiscus',
                            r'r(é|e)sidentiel', r'zone 3', r'zone 4c', r'champroux', r'zone4', r'azala(i|ï)', r'marcory petit march(é|e)', r'pharmacie (e|é)bath(é|e)'],
                # Le Plateau
                'plateau': [r'plateau ind(é|e)ni(é|e)', r'plateau dokui', r'plateau vallon', r'plateau centre',
                            r'plateau nord',
                            r'plateau sud', r'plateau', r'zone ccia'],
                # bingerville
                'bingerville': [r'f(e|eh)kess(é|e)', r'residentiel', r'lauriers 18'

                            ],
                # Port-Bouët
                'port-bouët': [r'anani', r'gonzagueville', r'vridi', r'adjouffou', r'jean folly', r'port', r'petit bassam',
                               r'vridi canal'],
                # Treichville
                'treichville': [r'arras', r'belleville', r'zone 3', r'avenue 16', r'avenue 8', r'avenue 12', r'avenue 21'],
                # Yopougon
                'yopougon': [r'yopougon carrefour chu', r'ananeraie', r'niangon', r'sid(é|e)ci', r'toits rouges',
                             r'andokoi', r'selmer', r'maroc', r'kout(é|e)', r'wassakara', r'sicogi', r'gesco', r'yopougon cite verte', r'yopougon belle cit(é|e)',
                             r'yopougon cité verte', r'yopougon academie', r'yopougon académie', r'yopougon cit(é|e) cie', r'lyc(é|e)e professionnel', r'azito']
            }

            for commune_key, quartiers in commune_to_quartier.items():
                for quartier_key in quartiers:
                    quartier_tokens = re.findall(r'\b\w+\b', quartier_key.lower())
                    if all(token in '|'.join([str(element["Libelle_du_produit"]), str(element["Commune"]), str(element["Quartier"])]).lower().replace(' ', '') for token in quartier_tokens):
                        element["Quartier"] = quartier_key
                        element["Commune"] = commune_key

            # Créer une liste de quartiers corrigés à partir de ce qui est là qui seront utilisés dans les dashboards

            if element["Commune"] not in commune_to_quartier.keys():
                element["Quartier"] = None
                element["Commune"] = None

            return element

        data_clean[["Libelle_du_produit", "Commune", "Quartier"]]= data_clean[["Libelle_du_produit", "Commune", "Quartier"]].apply(lambda row: traitement_quartier_commune(row), axis=1)

        return data_clean


    data_clean = commune_traitement()
    data_final = data_clean.dropna(subset=["Superficie", "Prix_du_produit", "Type_immobilier", "Quartier", "Commune"])


    donnee = data_final
    #print(donnee["Superficie"][donnee[["Superficie", "Prix_du_produit", "Type_immobilier"]].notna().all(axis=1)])

    # Spliter le chemin d'accès file pour utiliser
    file_split= file.split("\\")

    fichier_sortie = os.path.join(
        "D:\\", "DOCUMENTS", "CAE", "Scraping", "Loyer_par_m2_abj", "Prix_logements", "Workflow", "Traitement", file_split[-2], file_split[-1]
    )

    os.makedirs(os.path.dirname(fichier_sortie), exist_ok=True)
    donnee.to_excel(fichier_sortie, index=False)
    # Renommer le fichier traité dans data_collecte en ajoutant la mention 'traité'
    os.rename(file, file.replace(".xls", "_traité.xls"))


# Parcourir tous les fichiers non encore pré-traités et les pré-traiter
directory = rf"D:\DOCUMENTS\CAE\Scraping\Loyer_par_m2_abj\Prix_logements\Workflow\Data_Collecte"
all_files = []

# Parcours récursif des fichiers dans le dossier
for root, dirs, files in os.walk(directory):
    for file in files:
        if not (file.endswith('_traité.xlsx') or file.endswith('_traité.xls')):
            file_path = os.path.join(root, file)
            all_files.append(file_path)



for file in all_files:
    # Lire le fichier Excel
    data = pd.read_excel(file, engine="openpyxl")
    preprocessing(data, file)





