# -*- coding: utf-8 -*-
from asyncio.windows_events import NULL
from cgi import test
import re
import tkinter.scrolledtext as ScrolledText
from tkinter import messagebox, font
from datetime import datetime
from tqdm import tqdm
import tkinter as tk
import configparser
import os
os.environ["NLS_LANG"] = ".AL32UTF8"
import cx_Oracle
import threading
import requests
import schedule
import logging
import tkinter
import json
import time
import sys

settings = configparser.ConfigParser()
settings.read('settings.ini')

config = configparser.ConfigParser()
config.read('config.ini')

CRM_USERS_URL = "https://api.crmsimples.com.br/API?method=saveContato"
CRM_USERS_STATUS_URL = "https://api.crmsimples.com.br/API?method=saveContatoStatus"
CRM_NEGOTIATIONS_URL = "https://api.crmsimples.com.br/API?method=saveNegociacao"
CRM_NEGOTIATIONS_STATUS_URL = "https://api.crmsimples.com.br/API?method=saveNegociacaoStatus"
CRM_GET_USERS_URL = "https://api.crmsimples.com.br/API?method=getContato"
CRM_RESPONSAVEL_URL = "https://api.crmsimples.com.br/API?method=changeContatoResponsavel"

SAGA_VALIDATION_URL = "https://id.sagasistemas.com.br/api/v1/token"

restar_routine = True

def import_query(filename):
    """opens sql file and returns a list with all queries inside"""
    file = open(filename)
    full_sql = file.read()
    sql_commands = full_sql.split(";")
    return sql_commands

def database_connect():
    """starts oracle client and returns connection object"""
    try:
        cx_Oracle.init_oracle_client(lib_dir=config['ORACLE']['oracle_client_config_file'])
    except:
        pass
    user = config['ORACLE']['user']
    password = config['ORACLE']['password']
    ip = config['ORACLE']['ip']
    sid = config['ORACLE']['sid']

    connection = cx_Oracle.connect(
        user,
        password,
        f'{ip}/{sid}')

    return connection

def makeDictFactory(cursor):
    """function used to change the cursor 'fetchall' return to dictionary"""
    columnNames = [d[0] for d in cursor.description]
    def createRow(*args):
        return dict(zip(columnNames, args))
    return createRow

def set_cursor_return_as_dict(cursor):
    """replaces the cursor rowfactory function with the one that returns a dictionary"""
    cursor.rowfactory = makeDictFactory(cursor)
    return cursor

def send_data_to_crm(endpoint, payload):
    #print("Sending contact/negotiation")
    REQUESTS_PER_MINUTE = 50
    logging.info(datetime.now().strftime("%H:%M:%S - ") + "sending data to CRM")
    headers_dict = {
        "token": config['CRM']['token']
    }
    #saida = open("saida.txt","w+")
    
    start_time = datetime.now()
    request_counter = 0

    for index, item in enumerate(payload):
        if(request_counter >= REQUESTS_PER_MINUTE):
            end_time = datetime.now()

            if((end_time - start_time).seconds >= 60):
                start_time = datetime.now()
                request_counter = 0
            else:
                seconds_to_wait = 60 - (end_time - start_time).seconds
                logging.info(f'Esperando {seconds_to_wait} segundos...')
                start_time = datetime.now()
                request_counter = 0
                time.sleep(seconds_to_wait)

        json_item = json.dumps(item, indent = 4, default=str, ensure_ascii=False)
        
        #saida.write(json_item)
        
        result = requests.post(endpoint, data=json_item, headers=headers_dict)
        time.sleep(0.5)

        logging.info(f'{index + 1}/{len(payload)} sincronizados') if result.status_code == 200 else logging.error(f'erro ao sincronizar item :{result.text if result != None else ""}')
        request_counter += 1
    #saida.close()

def send_serialized_data_to_crm(endpoint, payload):
    #print("Sending status")
    #saida = open("saida_status.txt","w")
    logging.info(datetime.now().strftime("%H:%M:%S - ") + "sending data to CRM")
    headers_dict = {
        "token": config['CRM']['token'],
        "Content-Type": "application/json"
    }

    json_item = json.dumps(payload, indent = 4, default=str, ensure_ascii=False)

    #saida.write(json_item)

    result = requests.post(endpoint, data=json_item, headers=headers_dict)
    logging.info(f'{len(payload)} sincronizados') if result.status_code == 200 else logging.error(f'erro ao sincronizar item :{result.text if result != None else ""}')

    #saida.close()


def replace_column_names(result_query, column_names):
    result_dict_list = []
    for row in result_query: # rows list
        row_dict = {}
        for row_column, column_name in zip(row, column_names):
            row_dict[column_name] = row[row_column]
            result_dict_list.append(row_dict)
    return result_query

def get_contact(idExterno,cnpjCpf):
    endpoint = CRM_GET_USERS_URL
    if idExterno:
        endpoint = endpoint+'&idExterno='+idExterno
    if cnpjCpf:
        endpoint = endpoint+'&cnpjCpf='+cnpjCpf
    
    #print(endpoint)
    print("Getting contact")
    logging.info(datetime.now().strftime("%H:%M:%S - ") + "requesting contact from CRM")

    headers_dict = {
        "token": config['CRM']['token'],
        "Content-Type": "application/json"
    }
    payload={}
    #json_item = json.dumps(payload, indent = 4, default=str, ensure_ascii=False)
    response = requests.request("GET", endpoint, headers=headers_dict, data=payload)
    result = response.json()
    
    #print((result.keys()))
    #print(result['IdExterno'])

    return result

    #logging.info(f'{len(payload)} sincronizados') if result.status_code == 200 else logging.error(f'erro ao sincronizar item :{result.text if result != None else ""}')



#####################################################################################################################################################
#CONTATOS
#####################################################################################################################################################
def replace_contact_column_names(result_query):
    final_client_list = []
    group_by_externalId_dict = {}
    for row in result_query: # rows list
        if row["IDEXTERNO"] not in group_by_externalId_dict:
            group_by_externalId_dict[row["IDEXTERNO"]] = []
        group_by_externalId_dict[row["IDEXTERNO"]].append(row)

    for key in group_by_externalId_dict.keys(): # iteration in external key
        first_externalId_row = group_by_externalId_dict[key][0]

        #Pega o status do contato do arquivo de configuração pelo codigo/descrição
        status = settings['STATUS'][str(first_externalId_row["STATUSCONTATO"])]
        teste_contato = open("teste_contato.txt","w")
        teste_contato.write(get_contact(NULL,first_externalId_row["CNPJCPF"])['IdExterno'])
        teste_contato.close()
        #print(get_contact(NULL,first_externalId_row["CNPJCPF"])['IdExterno'])
        client = {
            "idExterno": first_externalId_row["IDEXTERNO"],
            "nome": first_externalId_row["NOME"],
            "tipoPessoa": first_externalId_row["TIPOPESSOA"].encode('iso-8859-1').decode(),
            "cnpjCpf": first_externalId_row["CNPJCPF"].encode('iso-8859-1').decode(),
            "organizacao": {
                "idExterno": "0"
                },
            "fonteContato": first_externalId_row["FONTECONTATO"].encode('iso-8859-1').decode() if first_externalId_row["FONTECONTATO"] else None,
            "statusContato": status.encode('iso-8859-1').decode() if first_externalId_row["STATUSCONTATO"] else None,
            "dataNascimento": first_externalId_row["DATANASCIMENTO"],
            "observacoes": first_externalId_row["OBSERVACOES"].encode('iso-8859-1').decode() if first_externalId_row["OBSERVACOES"] else None,
            "visivelPara": first_externalId_row["VISIVELPARA"].encode('iso-8859-1').decode() if first_externalId_row["VISIVELPARA"] else None,
            "ranking": first_externalId_row["RANKING"],
            "score": first_externalId_row["SCORE"],
            "idUsuarioInclusao": first_externalId_row["IDUSUARIOINCLUSAO"],
            "idExternoUsuarioInclusao": first_externalId_row["IDEXTERNOUSUARIOINCLUSAO"],
            "contatoDesde": first_externalId_row["CONTATODESDE"],
            "listEndereco": [],
            "listFone": [],
            "listEmail": [],
            "listOutrosContatos": [],
            "listIdExternoResponsaveis": [],
            "listTags": [],
        }

        for client_row in group_by_externalId_dict[key]:

            if(client_row["SELECTTIPOENDERECO"] != None):
                client_address_obj = {
                    "selectTipoEndereco": client_row["SELECTTIPOENDERECO"].encode('iso-8859-1').decode() if client_row["SELECTTIPOENDERECO"] else None,
                    "endereco": client_row["ENDERECO"].encode('iso-8859-1').decode() if client_row["ENDERECO"] else None,
                    "numero": client_row["NUMERO"],
                    "complemento": client_row["COMPLEMENTO"],
                    "bairro": client_row["BAIRRO"].encode('iso-8859-1').decode() if client_row["BAIRRO"] else None,
                    "cidade": client_row["CIDADE"].encode('iso-8859-1').decode() if client_row["CIDADE"] else None,
                    "uf": client_row["UF"].encode('iso-8859-1').decode() if client_row["UF"] else None,
                    "cep": client_row["CEP"],
                    "pais": client_row["PAIS"].encode('iso-8859-1').decode() if client_row["PAIS"] else None,
                }
                if(client_address_obj not in client["listEndereco"]):
                    client["listEndereco"].append(client_address_obj)

            if(client_row["DESCRICAO_FONE"] != None):
                client_phone_obj = {
                    "descricao": client_row["DESCRICAO_FONE"].encode('iso-8859-1').decode() if client_row["DESCRICAO_FONE"] else None,
                    "selectTipo": client_row["SELECTTIPO_FONE"]
                }
                if(client_phone_obj not in client["listFone"]):
                    client["listFone"].append(client_phone_obj)

            if(client_row["DESCRICAO_EMAIL"] != None):
                emails = client_row["DESCRICAO_EMAIL"].encode('iso-8859-1').decode()
                emails.replace(';',',')
                for email in client_row["DESCRICAO_EMAIL"].encode('iso-8859-1').decode().split(','):
                    client_email_obj = {
                        "descricao": email.strip() if email.strip() else None,
                        "selectTipo": client_row["SELECTTIPO_EMAIL"].encode('iso-8859-1').decode() if client_row["SELECTTIPO_EMAIL"] else None
                    }
                    if(client_email_obj not in client["listEmail"]):
                        client["listEmail"].append(client_email_obj)

            #VER SE ESSE TESTE FAZ SENTIDO - GABI
            '''if(client["listOutrosContatos"] != None 
                    and client_row["NOME_CONTATO"] != None
                    and client_row["CARGORELACAO"] != None
                    and client_row["FONE"] != None
                    and client_row["EMAIL"] != None):'''

            if(client_row["NOME_CONTATO"] != None):
                contato = {
                    "nome" : client_row["NOME_CONTATO"].encode('iso-8859-1').decode() if client_row["NOME_CONTATO"] else None,
                    "cargoRelacao" : client_row["CARGORELACAO"].encode('iso-8859-1').decode() if client_row["CARGORELACAO"] else None,
                    "fone" : client_row["FONE"],
                    "email" : client_row["EMAIL"].encode('iso-8859-1').decode() if client_row["EMAIL"] else None
                }
                if(contato not in client["listOutrosContatos"]):
                    client["listOutrosContatos"].append(contato)

            if(client_row["LISTIDEXTERNORESPONSAVEIS"] != None
                and client_row["LISTIDEXTERNORESPONSAVEIS"] not in client["listIdExternoResponsaveis"]):
                client["listIdExternoResponsaveis"].append(client_row["LISTIDEXTERNORESPONSAVEIS"])

            if(client_row["IDTAG"] != None
                and client_row["DESCTAG"] != None):
                des_tag = settings["TAGS"][str(client_row["IDTAG"])]
                listTags = {
                    "id": client_row["IDTAG"],
                    "descricao": des_tag.encode('iso-8859-1').decode()
                }
                if(listTags not in client["listTags"]):
                    client["listTags"].append(listTags)

        final_client_list.append(client)
    #print(final_client_list)
    print("Sending Contacts")
    saida_contato = open("saida_contato.txt","w")
    saida_contato.write(str(final_client_list))
    saida_contato.close()

    #result = send_data_to_crm(CRM_USERS_URL, final_client_list)
    #logging.info(datetime.now().strftime("%H:%M:%S - ") + result.json() if result != None else "")

#####################################################################################################################################################
#STATUS CONTATO
#####################################################################################################################################################
def replace_contact_status_column_names(result_query):
    final_client_list = []
    group_by_externalId_dict = {}
    for row in result_query: # rows list
        if row["IDEXTERNO"] not in group_by_externalId_dict:
            group_by_externalId_dict[row["IDEXTERNO"]] = []
        group_by_externalId_dict[row["IDEXTERNO"]].append(row)

    for key in group_by_externalId_dict.keys(): # iteration in external key
        first_externalId_row = group_by_externalId_dict[key][0]
        status = settings['STATUS'][str(first_externalId_row["STATUSCONTATO"])]
        client = {
            "idExterno": first_externalId_row["IDEXTERNO"],
            "newStatusContato": status.encode('iso-8859-1').decode() if first_externalId_row["STATUSCONTATO"] else None,
        }

        final_client_list.append(client)
    #print(final_client_list)
    print("Sending Status Contact")
    saida_status_contato = open("saida_status_contato.txt","w")
    saida_status_contato.write(str(final_client_list))
    saida_status_contato.close()

    result = send_serialized_data_to_crm(CRM_USERS_STATUS_URL, final_client_list)
    logging.info(datetime.now().strftime("%H:%M:%S - ") + result.json() if result != None else "")

#####################################################################################################################################################
#RESPONSAVEIS
#####################################################################################################################################################
def replace_contact_responsaveis_column_names(result_query):
    final_client_list = []
    group_by_externalId_dict = {}
    for row in result_query: # rows list
        if row["IDEXTERNO"] not in group_by_externalId_dict:
            group_by_externalId_dict[row["IDEXTERNO"]] = []
        group_by_externalId_dict[row["IDEXTERNO"]].append(row)

    for key in group_by_externalId_dict.keys(): # iteration in external key
        first_externalId_row = group_by_externalId_dict[key][0]
        status = settings['STATUS'][str(first_externalId_row["STATUSCONTATO"])]

        #Pega o contato
        #print(first_externalId_row["IDEXTERNO"],first_externalId_row["CNPJCPF"])
        get_contact(first_externalId_row["IDEXTERNO"],first_externalId_row["CNPJCPF"])

        client = {
            "idExterno": first_externalId_row["IDEXTERNO"],
            "newStatusContato": status.encode('iso-8859-1').decode() if first_externalId_row["STATUSCONTATO"] else None,
        }

        final_client_list.append(client)
    #print(final_client_list)
    
    result = send_serialized_data_to_crm(CRM_USERS_STATUS_URL, final_client_list)
    logging.info(datetime.now().strftime("%H:%M:%S - ") + result.json() if result != None else "")

#####################################################################################################################################################
#STATUS NEGOCIACAO
#####################################################################################################################################################
def replace_negotiation_status_column_names(result_query):
    final_negotiation_list = []
    group_by_externalId_dict = {}
    for row in result_query: # rows list
        if row["IDEXTERNONEGOCIACAO"] not in group_by_externalId_dict:
            group_by_externalId_dict[row["IDEXTERNONEGOCIACAO"]] = []
        group_by_externalId_dict[row["IDEXTERNONEGOCIACAO"]].append(row)

    for key in group_by_externalId_dict.keys(): # iteration in external key
        first_externalId_row = group_by_externalId_dict[key][0]
        #Pega o motivo de mudança de status da negociação do arquivo de configuração pelo codigo/descrição
        motivo = settings['MOTIVO DE CANCELAMENTO'][str(first_externalId_row["MOTIVOGANHOPERDA"])]

        negotiation = {
            "idExternoContato": first_externalId_row["IDEXTERNOCONTATO"],
            "idExternoNegociacao": first_externalId_row["IDEXTERNONEGOCIACAO"],
            "newStatusNegociacao": first_externalId_row["STATUSNEGOCIACAO"].encode('iso-8859-1').decode() if first_externalId_row["STATUSNEGOCIACAO"] else None,
            "idExternoUsuarioConclusao": first_externalId_row["IDEXTERNOUSUARIOCONCLUSAO"], # para negociações Ganhas ou Perdidas
            "concluidaEm": first_externalId_row["CONCLUIDAEM"],
            "motivoGanhoPerda": motivo,
        }
        final_negotiation_list.append(negotiation)
    
    #print(final_negotiation_list)
    print("Sending Negotiation")
    saida_negociacao = open("saida_negociacao.txt","w")
    saida_negociacao.write(str(final_negotiation_list))
    saida_negociacao.close()
    
    result = send_data_to_crm(CRM_NEGOTIATIONS_STATUS_URL, final_negotiation_list)
    logging.info(datetime.now().strftime("%H:%M:%S - ") + result.json() if result != None else "")


######################################################################################################################################################
#NEGOCIACAO
#####################################################################################################################################################
def replace_negotiation_column_names(result_query):
    final_negotiation_list = []
    group_by_externalId_dict = {}
    for row in result_query: # rows list
        if row["IDEXTERNO"] not in group_by_externalId_dict:
            group_by_externalId_dict[row["IDEXTERNO"]] = []
        group_by_externalId_dict[row["IDEXTERNO"]].append(row)

    for key in group_by_externalId_dict.keys(): # iteration in external key
        negotiation_organizations = {}
        for negotiation_row in group_by_externalId_dict[key]:
            if(negotiation_row["IDEXTERNO_ORGANIZACAO"] not in negotiation_organizations):
                negotiation_organizations[negotiation_row["IDEXTERNO_ORGANIZACAO"]] = []
            negotiation_organizations[negotiation_row["IDEXTERNO_ORGANIZACAO"]].append(negotiation_row)

        for negotiation_organization_key in negotiation_organizations.keys():
            first_organization_row = negotiation_organizations[negotiation_organization_key][0]
            # sets all static data from row and create the lists to receive the rest
            categoria = settings["TAGS"][str(first_organization_row["CATEGORIANEGOCIACAO"])]
            notafiscal = str(first_organization_row["NOTAFISCAL"]).replace(',',' ')
            negotiation = {
                "idExterno": first_organization_row["IDEXTERNO"],
                "contato": {
                    "idExterno": first_organization_row["IDEXTERNO_CONTATO"]
                },
                "organizacao": {
                    "idExterno": first_organization_row["IDEXTERNO_ORGANIZACAO"]
                    },
                "nome": first_organization_row["NOME"].encode('iso-8859-1').decode() if first_organization_row["NOME"] else None,
                "descricao": first_organization_row["DESCRICAO"].encode('iso-8859-1').decode() if first_organization_row["DESCRICAO"] else None,
                "categoriaNegociacao": categoria.encode('iso-8859-1').decode() if first_organization_row["CATEGORIANEGOCIACAO"] else None,
                "idEtapaNegociacao": first_organization_row["IDETAPANEGOCIACAO"],
                "statusNegociacao": first_organization_row["STATUSNEGOCIACAO"].encode('iso-8859-1').decode() if first_organization_row["STATUSNEGOCIACAO"] else None,
                "valor": first_organization_row["VALOR"],
                "idExternoUsuarioInclusao": first_organization_row["IDEXTERNOUSUARIOINCLUSAO"],
                "criadaEm": first_organization_row["CRIADAEM"],
                "listProduto": [],
                "listCampoUsuario": [{"nomeCampo": "Nota Fiscal", "valor": notafiscal if first_organization_row["NOTAFISCAL"] else None}],
                "listIdExternoResponsaveis": [first_organization_row["IDEXTERNOUSUARIOINCLUSAO"]if first_organization_row["IDEXTERNOUSUARIOINCLUSAO"] else None],
                "listTags": [],
                "previsaoFechamento": first_organization_row["PREVISAOFECHAMENTO"],
            }

            for negotiation_row in negotiation_organizations[negotiation_organization_key]:
                if(negotiation_row["IDEXTERNO_PRODUTO"] != None):
                    negotiation_product_obj = {
                        "produto": {
                            "idExterno": negotiation_row["IDEXTERNO_PRODUTO"],
                        },
                        "valorUnitario": negotiation_row["VALORUNITARIO"] if negotiation_row["VALORUNITARIO"] != None else 0,
                        "quantidade": negotiation_row["QUANTIDADE"] if negotiation_row["QUANTIDADE"] != None else 0,
                        "percentualDesconto": negotiation_row["PERCENTUALDESCONTO"] if negotiation_row["PERCENTUALDESCONTO"] != None else 0,
                        "valorTotal": negotiation_row["VALORTOTAL"] if negotiation_row["VALORTOTAL"] != None else 0,
                        "comentarios": negotiation_row["COMENTARIOS"].encode('iso-8859-1').decode() if first_organization_row["COMENTARIOS"] else None,
                        "anotacao": negotiation_row["ANOTACAO"].encode('iso-8859-1').decode() if first_organization_row["ANOTACAO"] else None,
                    }
                    if(negotiation_product_obj not in negotiation["listProduto"]):
                        negotiation["listProduto"].append(negotiation_product_obj)
                        
            final_negotiation_list.append(negotiation)
    #print(final_negotiation_list)
    print("Sending Status Negotiation")
    saida_status_negociacao = open("saida_status_negociacao.txt","w")
    saida_status_negociacao.write(str(final_negotiation_list))
    saida_status_negociacao.close()

    send_data_to_crm(CRM_NEGOTIATIONS_URL, final_negotiation_list)


def validate_client_permission():
    payload = {"app":"integracao-crm-simples","password":config['SAGA_VALIDATION']['password'],"username": config['SAGA_VALIDATION']['username']}
    payload = json.dumps(payload, indent = 4, default = str, ensure_ascii=False)
    headers_dict = {
      'Content-Type': 'application/json'
    }

    result = requests.post(SAGA_VALIDATION_URL, data=payload, headers=headers_dict)
    if(result.status_code == 200):
        return True
    return False

def synchronize_data(cursor, sql_file, url_crm):
    logging.info(datetime.now().strftime("%H:%M:%S - ") + "Obtendo dados do Banco Oracle")
    sql_commands = import_query(sql_file)
    cursor.execute(sql_commands[0])
    cursor = set_cursor_return_as_dict(cursor)
    query_result = cursor.fetchall()
    # logging.error(len(query_result))
    logging.info(datetime.now().strftime("%H:%M:%S - ") + "Dados obtidos com sucesso")

    if(sql_file == config['SQL_QUERIES']['clients']):
        #pass
        replace_contact_column_names(query_result)

    elif(sql_file == config['SQL_QUERIES']['clients_status']):
        #GABI
        replace_contact_status_column_names(query_result)

    elif(sql_file == config['SQL_QUERIES']['negotiations']):
        #pass
        replace_negotiation_column_names(query_result)
    
    if(sql_file == config['SQL_QUERIES']['negotiations_status']):
        #GABI
        replace_negotiation_status_column_names(query_result)
    #elif(sql_file == config['SQL_QUERIES']['clientsUpdate']):
    #    logging.info(query_result)
    

class TextHandler(logging.Handler):
    def __init__(self, text):
        logging.Handler.__init__(self)
        self.text = text

    def emit(self, record):
        msg = self.format(record)
        def append():
            self.text.configure(state='normal')
            self.text.insert(tk.END, msg + '\n')
            self.text.configure(state='disabled')
            self.text.yview(tk.END)
        self.text.after(0, append)

class myGUI(tk.Frame):
    def __init__(self, parent, *args, **kwargs):
        tk.Frame.__init__(self, parent, *args, **kwargs)
        self.root = parent
        self.build_gui()

    def build_gui(self):
        # Build GUI
        self.root.title('integration_oracle_crmsimples')
        self.root.option_add('*tearOff', 'FALSE')
        self.grid(column=0, row=0, sticky='ew')
        self.grid_columnconfigure(0, weight=1, uniform='a')
        self.grid_columnconfigure(1, weight=1, uniform='a')
        self.grid_columnconfigure(2, weight=1, uniform='a')
        self.grid_columnconfigure(3, weight=1, uniform='a')

        # Add text widget to display logging info
        st = ScrolledText.ScrolledText(self, state='disabled')
        st.configure(font='Arial')
        st.grid(column=0, row=1, sticky='w', columnspan=4)

        # Create textLogger
        text_handler = TextHandler(st)

        # Logging configuration
        logging.basicConfig(filename='application.log',
            level=logging.INFO,
            format='%(asctime)s - %(message)s')

        # Add the handler to logger
        logger = logging.getLogger()
        logger.addHandler(text_handler)

def worker():
    logging.info(datetime.now().strftime("%H:%M:%S - ") + "Bem-vindo ao Integrador Saga Sistemas")
    logging.info(datetime.now().strftime("%H:%M:%S - ") + "Validando login de cliente")
    #if(validate_client_permission()):
    logging.info(datetime.now().strftime("%H:%M:%S - ") + "Login validado")
    logging.info(datetime.now().strftime("%H:%M:%S - ") + "Iniciando conexão com o banco Oracle")
    connection = database_connect()
    logging.info(datetime.now().strftime("%H:%M:%S - ") + "Conexão estabelecida")
    cur = connection.cursor()

    logging.info(datetime.now().strftime("%H:%M:%S - ") + "Iniciando sincronização de dados de clientes")
    synchronize_data(cur, config['SQL_QUERIES']['clients'], CRM_USERS_URL)
    logging.info(datetime.now().strftime("%H:%M:%S - ") + "Sincronização de clientes completa")
    
    logging.info(datetime.now().strftime("%H:%M:%S - ") + "Iniciando sincronização de status de clientes")
    #synchronize_data(cur, config['SQL_QUERIES']['clients_status'], CRM_USERS_STATUS_URL)
    logging.info(datetime.now().strftime("%H:%M:%S - ") + "Sincronização de status de clientes completa")
    
    logging.info(datetime.now().strftime("%H:%M:%S - ") + "Iniciando sincronização de dados de negociações")
    #synchronize_data(cur, config['SQL_QUERIES']['negotiations'], CRM_NEGOTIATIONS_URL)
    logging.info(datetime.now().strftime("%H:%M:%S - ") + "Sincronização de negociações completa")
    
    logging.info(datetime.now().strftime("%H:%M:%S - ") + "Iniciando sincronização de status de negociações")
    #synchronize_data(cur, config['SQL_QUERIES']['negotiations_status'], CRM_NEGOTIATIONS_STATUS_URL)
    logging.info(datetime.now().strftime("%H:%M:%S - ") + "Sincronização de status de negociações completa")
    #logging.info(datetime.now().strftime("%H:%M:%S - ") + "Iniciando processo para setar no ERP clientes já importados")
    
    #synchronize_data(cur, config['SQL_QUERIES']['clientsUpdate'],False)
    #sql_commands = import_query(config['SQL_QUERIES']['clientsUpdate'])
    #logging.info(sql_commands)
    #a = cur.execute(sql_commands[0])
    #logging.info(a)
    #logging.info(datetime.now().strftime("%H:%M:%S - ") + "Processo para setar clientes já importados completo")
    connection.close()
    logging.info(datetime.now().strftime("%H:%M:%S - ") + "Conexão com o banco Oracle encerrada")
    #else:
        #logging.error("O cliente não possui permissão para executar a operação")
    try:
        pass
    except Exception as exception:
        logging.info(datetime.now().strftime("%H:%M:%S - ") + f'{exception}, por favor reinicie a aplicação.')
        logging.info(datetime.now().strftime("%H:%M:%S - ") + f'{type(exception)}')


def start_schedule():
    schedule.every(int(config['SCHEDULE']['time_gap'])).minutes.do(worker)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    root = tk.Tk()
    myGUI(root)

    t1 = threading.Thread(target=worker, args=()).start()

    #threading.Thread(target=start_schedule, args=()).start()

    root.protocol("WM_DELETE_WINDOW", sys.exit)
    root.mainloop()
