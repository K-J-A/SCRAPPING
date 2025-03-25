from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from selenium import webdriver
import pandas as pd
from bs4 import BeautifulSoup
import re
import numpy as np
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, WebDriverException
import sys
import os
# Add the parent directory of 'modules' to the Python path
import logging
import traceback

#from Script_Scrapping_jumia import fetch_jumia_data
from send_mail import send_mail_success, send_mail_error, send_mail_error1
from Script_Scrapping_cpi import scrapping_AIK
#from Script_Scrapping_cpi import fetch_jumia_data

from Script_df_abidjan_net import df_abidjan_net
from Script_df_batirici import df_batirici
from Script_df_Coinafrique_CI import df_coinafrique_CI
from Script_df_Expat import df_expat
from Script_df_ha_properties import df_ha_properties
from Script_df_immobilier_CI import df_immobilier_CI

date_debut = datetime.strptime(datetime.now().strftime('%m-%Y'), "%m-%Y")

mois_en_cours=date_debut.strftime("%m%Y")                                                                                                                 
# Fonction de scraping JUMIA



# Fonction de scraping abidjan_net
def scrap_abidjan_net(**kwargs):
    try:

        df1 = df_abidjan_net()

        # Réaffectation des colonnes dans le nouvel ordre
        df1 = df1[
            ['Date_de_collecte', 'Code_site', 'Libelle_du_produit', 'Superficie', "Prix_du_produit", 'Quartier',
             'Nombre_pieces', 'Type_immobilier', 'Commune']]

        # df1= pd.concat([df1, df2], ignore_index=True)
        # Pusher les résultats dans XCom
        kwargs['ti'].xcom_push(key='abidjan_net_data', value=df1.to_json())

    except Exception as e:
        send_mail_error1(["j.kouassi@stat.plan.gouv.ci", "abdoulayebakayoko265@gmail.com"],
                         ["j.migone@stat.plan.gouv.ci", "moussakr@gmail.com"], "scrap_abidjan_net")
        raise


# Fonction de scraping batirici
def scrap_batirici(**kwargs):
    try:

        df2 = df_batirici()

        # Réaffectation des colonnes dans le nouvel ordre
        df2 = df2[
            ['Date_de_collecte', 'Code_site', 'Libelle_du_produit', 'Superficie', "Prix_du_produit", 'Quartier',
             'Nombre_pieces', 'Type_immobilier', 'Commune']]

        # df1= pd.concat([df1, df2], ignore_index=True)
        # Pusher les résultats dans XCom
        kwargs['ti'].xcom_push(key='batirici_data', value=df2.to_json())

    except Exception as e:
        send_mail_error1(["j.kouassi@stat.plan.gouv.ci", "abdoulayebakayoko265@gmail.com"],
                         ["j.migone@stat.plan.gouv.ci", "moussakr@gmail.com"], "scrap_batirici")
        raise


# Fonction de scraping coinafrique_CI
def scrap_coinafrique_CI(**kwargs):
    try:

        df3 = df_coinafrique_CI()

        # Réaffectation des colonnes dans le nouvel ordre
        df3 = df3[
            ['Date_de_collecte', 'Code_site', 'Libelle_du_produit', 'Superficie', "Prix_du_produit", 'Quartier',
             'Nombre_pieces', 'Type_immobilier', 'Commune']]

        # df1= pd.concat([df1, df2], ignore_index=True)
        # Pusher les résultats dans XCom
        kwargs['ti'].xcom_push(key='coinafrique_CI_data', value=df3.to_json())

    except Exception as e:
        send_mail_error1(["j.kouassi@stat.plan.gouv.ci", "abdoulayebakayoko265@gmail.com"],
                         ["j.migone@stat.plan.gouv.ci", "moussakr@gmail.com"], "scrap_coinafrique_CI")
        raise


# Fonction de scraping expat
def scrap_expat(**kwargs):
    try:

        df4 = df_expat()

        # Réaffectation des colonnes dans le nouvel ordre
        df4 = df4[
            ['Date_de_collecte', 'Code_site', 'Libelle_du_produit', 'Superficie', "Prix_du_produit", 'Quartier',
             'Nombre_pieces', 'Type_immobilier', 'Commune']]

        # df1= pd.concat([df1, df2], ignore_index=True)
        # Pusher les résultats dans XCom
        kwargs['ti'].xcom_push(key='expat_data', value=df4.to_json())

    except Exception as e:
        send_mail_error1(["j.kouassi@stat.plan.gouv.ci", "abdoulayebakayoko265@gmail.com"],
                         ["j.migone@stat.plan.gouv.ci", "moussakr@gmail.com"], "scrap_expat")
        raise



# Fonction de scraping ha_properties
def scrap_ha_properties(**kwargs):
    try:

        df5 = df_ha_properties()

        # Réaffectation des colonnes dans le nouvel ordre
        df5 = df5[
            ['Date_de_collecte', 'Code_site', 'Libelle_du_produit', 'Superficie', "Prix_du_produit", 'Quartier',
             'Nombre_pieces', 'Type_immobilier', 'Commune']]

        # df1= pd.concat([df1, df2], ignore_index=True)
        # Pusher les résultats dans XCom
        kwargs['ti'].xcom_push(key='ha_properties_data', value=df5.to_json())

    except Exception as e:
        send_mail_error1(["j.kouassi@stat.plan.gouv.ci", "abdoulayebakayoko265@gmail.com"],
                         ["j.migone@stat.plan.gouv.ci", "moussakr@gmail.com"], "scrap_ha_properties")
        raise



# Fonction de scraping immobilier_CI
def scrap_immobilier_CI(**kwargs):
    try:

        df6 = df_immobilier_CI()

        # Réaffectation des colonnes dans le nouvel ordre
        df6 = df6[
            ['Date_de_collecte', 'Code_site', 'Libelle_du_produit', 'Superficie', "Prix_du_produit", 'Quartier',
             'Nombre_pieces', 'Type_immobilier', 'Commune']]

        # df1= pd.concat([df1, df2], ignore_index=True)
        # Pusher les résultats dans XCom
        kwargs['ti'].xcom_push(key='immobilier_CI_data', value=df6.to_json())

    except Exception as e:
        send_mail_error1(["j.kouassi@stat.plan.gouv.ci", "abdoulayebakayoko265@gmail.com"],
                         ["j.migone@stat.plan.gouv.ci", "moussakr@gmail.com"], "scrap_immobilier_CI")
        raise


def merge_and_save_data(**kwargs):
    try:
        ti = kwargs['ti']
        dfs_succes = []
        
        # Initialisation du logging
        logging.basicConfig(level=logging.INFO)
        
        # Liste des task_ids et leurs clés XCom correspondantes
        tasks = {
            'scrap_abidjan_net': 'abidjan_net_data',
            'scrap_batirici': 'batirici_data',
            'scrap_coinafrique_CI': 'coinafrique_CI_data',
            'scrap_expat': 'expat_data',
            'scrap_ha_properties': 'ha_properties_data',
            'scrap_immobilier_CI': 'immobilier_CI_data'
        }
        
        # Parcourir les tâches pour récupérer les données XCom
        for task_id, xcom_key in tasks.items():
            try:
                # Récupérer les données depuis XCom
                data_json = ti.xcom_pull(task_ids=task_id, key=xcom_key)
                
                # Vérifier si des données existent (données non nulles et non vides)
                if data_json:
                    df = pd.read_json(data_json)
                    
                    # Vérifier si le dataframe n'est pas vide
                    if not df.empty:
                        dfs_succes.append(df)
                        logging.info(f"Les données de {task_id} ont été ajoutées.")
                    else:
                        logging.warning(f"Le dataframe de {task_id} est vide. Il ne sera pas ajouté.")
                else:
                    logging.warning(f"Pas de données pour {task_id}.")
                    
            except Exception as e:
                logging.error(f"Erreur lors de la récupération des données de {task_id}: {e}")
                # Vous pouvez décider d'envoyer un email ou d'ignorer l'erreur selon le contexte

        # Fusionner les dataframes non vides
        if dfs_succes:
            df_final = pd.concat(dfs_succes, axis=0, ignore_index=True)
            fichier_sortie = os.path.join(
                "/mnt", "c", "airflow", "dags", "Prix_logements", "Workflow", "Data_Collecte", mois_en_cours,
                f"Data_Scrapping_{datetime.now().strftime('%d%m%Y')}.xlsx"
            )
            os.makedirs(os.path.dirname(fichier_sortie), exist_ok=True)
            df_final.to_excel(fichier_sortie, index=False)
            logging.info("Données fusionnées et sauvegardées avec succès.")
            
            # Envoi d'un email de succès
            send_mail_success(
                ["j.kouassi@stat.plan.gouv.ci", "abdoulayebakayoko265@gmail.com"], 
                ["j.migone@stat.plan.gouv.ci", "moussakr@gmail.com"]
            )
        else:
            logging.warning("Aucun dataframe valide à fusionner.")
            
    except Exception as e:
        logging.error(f"merge_and_save_data - Erreur: {traceback.format_exc()}")
        send_mail_error1(
            ["j.kouassi@stat.plan.gouv.ci", "abdoulayebakayoko265@gmail.com"], 
            ["j.migone@stat.plan.gouv.ci", "moussakr@gmail.com"], 
            f"merge_and_save_data - Erreur: {traceback.format_exc()}"
        )
        raise




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date' : datetime(2024, 8, 5),
    'email': ['bakayokoabdoulaye2809@gmail.com'],
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'WC_prix_logements',
    default_args=default_args,
    description='Workflow pour le scrapping des prix',
    catchup=False,
    schedule_interval ='30 14 */15 * *',
    start_date=datetime(2024, 11, 12),
)

'''
task1 = PythonOperator(
    task_id='scrap_jumia',
    python_callable=scrap_jumia,
    provide_context=True,
    dag=dag,
)
'''

task1 = PythonOperator(
    task_id='scrap_abidjan_net',
    python_callable= scrap_abidjan_net,
    provide_context=True,
    dag=dag,
    trigger_rule='all_done'
)

task2 = PythonOperator(
    task_id='scrap_batirici',
    python_callable= scrap_batirici,
    provide_context=True,
    dag=dag,
    trigger_rule='all_done'
)

task3 = PythonOperator(
    task_id='scrap_coinafrique_CI',
    python_callable= scrap_coinafrique_CI,
    provide_context=True,
    dag=dag,
    trigger_rule='all_done'
)

task4 = PythonOperator(
    task_id='scrap_expat',
    python_callable= scrap_expat,
    provide_context=True,
    dag=dag,
    trigger_rule='all_done'
)

task5 = PythonOperator(
    task_id='scrap_ha_properties',
    python_callable= scrap_ha_properties,
    provide_context=True,
    dag=dag,
    trigger_rule='all_done'
)

task6 = PythonOperator(
    task_id='scrap_immobilier_CI',
    python_callable= scrap_immobilier_CI,
    provide_context=True,
    dag=dag,
    trigger_rule='all_done'
)

task7 = PythonOperator(
    task_id='merge_and_save_data',
    python_callable=merge_and_save_data,
    provide_context=True,
    dag=dag,
    trigger_rule='all_done' 
)

task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7

