# -*- coding: utf-8 -*-

import tkinter.scrolledtext as ScrolledText
from tkinter import messagebox
from datetime import datetime
from tqdm import tqdm
import tkinter as tk
import configparser
import cx_Oracle
import threading
import requests
import schedule
import logging
import tkinter
import json
import time
import sys

config = configparser.ConfigParser()
config.read('config.ini')

CRM_USERS_URL = "https://api.crmsimples.com.br/API?method=saveContato"
CRM_NEGOTIATIONS_URL = "https://api.crmsimples.com.br/API?method=saveNegociacao"

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
    cx_Oracle.init_oracle_client(lib_dir=config['ORACLE']['oracle_client_config_file'])

    user = config['ORACLE']['user']
    password = config['ORACLE']['password']
    ip = config['ORACLE']['ip']
    sid = config['ORACLE']['sid']

    connection = cx_Oracle.connect(
        user,
        password,
        f'{ip}/{sid}',
        encoding="UTF-8")

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
    logging.info(datetime.now().strftime("%H:%M:%S - ") + "sending data to CRM")
    headers_dict = {
        "token": config['CRM']['token']
    }

    for index, item in enumerate(payload):
        json_item = json.dumps(item, indent = 4, default=str, ensure_ascii=False)
        # print(json_item)
        result = requests.post(endpoint, data=json_item, headers=headers_dict)

        logging.info(f'{index}/{len(payload)} sincronizados') if result.status_code == 200 else logging.error(f'erro ao sincronizar cliente de id:{result.json()["idExterno"]}')

def replace_column_names(result_query, column_names):
    result_dict_list = []
    for row in result_query: # rows list
        row_dict = {}
        for row_column, column_name in zip(row, column_names):
            row_dict[column_name] = row[row_column]
            result_dict_list.append(row_dict)
    return result_query

def replace_contact_column_names(result_query):
    final_client_list = []
    group_by_externalId_dict = {}
    for row in result_query: # rows list
        if row["IDEXTERNO"] not in group_by_externalId_dict:
            group_by_externalId_dict[row["IDEXTERNO"]] = []
        group_by_externalId_dict[row["IDEXTERNO"]].append(row)

    for key in group_by_externalId_dict.keys(): # iteration in external key
        client_organizations = {}
        for client_row in group_by_externalId_dict[key]:
            if(client_row["ORGANIZACAO"] not in client_organizations):
                client_organizations[client_row["ORGANIZACAO"]] = []
            client_organizations[client_row["ORGANIZACAO"]].append(client_row)

        for client_organization_key in client_organizations.keys():
            first_organization_row = client_organizations[client_organization_key][0]
            # sets all static data from row and create the lists to receive the rest
            client = {
                "idExterno": first_organization_row["IDEXTERNO"],
                "nome": first_organization_row["NOME"],
                "tipoPessoa": first_organization_row["TIPOPESSOA"],
                "cnpjCpf": first_organization_row["CNPJCPF"],
                "organizacao": {
                    "idExterno": first_organization_row["ORGANIZACAO"]
                    },
                "fonteContato": first_organization_row["FONTECONTATO"] if first_organization_row["FONTECONTATO"] else "",
                "statusContato": first_organization_row["STATUSCONTATO"],
                "dataNascimento": first_organization_row["DATANASCIMENTO"],
                "observacoes": first_organization_row["OBSERVACOES"],
                "visivelPara": first_organization_row["VISIVELPARA"],
                "ranking": first_organization_row["RANKING"],
                "score": first_organization_row["SCORE"],
                "idUsuarioInclusao": first_organization_row["IDUSUARIOINCLUSAO"],
                "idExternoUsuarioInclusao": first_organization_row["IDEXTERNOUSUARIOINCLUSAO"],
                "contatoDesde": first_organization_row["CONTATODESDE"],
                "listEndereco": [],
                "listFone": [],
                "listEmail": [],
                "listOutrosContatos": [],
                "listIdRepresentantes": [],
                "listIdExternoRepresentantes": [],
            }

            for client_row in client_organizations[client_organization_key]:
                if(client_row["DESCRICAO_FONE"] != None):
                    client_phone_obj = {
                        "descricao": client_row["DESCRICAO_FONE"],
                        "selectTipo": client_row["SELECTTIPO_FONE"]
                    }
                    if(client_phone_obj not in client["listFone"]):
                        client["listFone"].append(client_phone_obj)

                if(client_row["DESCRICAO_EMAIL"] != None):
                    client_email_obj = {
                        "descricao": client_row["DESCRICAO_EMAIL"],
                        "selectTipo": client_row["SELECTTIPO_EMAIL"]
                    }
                    if(client_email_obj not in client["listEmail"]):
                        client["listEmail"].append(client_email_obj)

                if(client["listOutrosContatos"] != None
                        and client_row["NOME_CONTATO"] != None
                        and client_row["CARGORELACAO"] != None
                        and client_row["FONE"] != None
                        and client_row["EMAIL"] != None):

                    contato = {
                        "nome" : client_row["NOME_CONTATO"],
                        "cargoRelacao" : client_row["CARGORELACAO"],
                        "fone" : client_row["FONE"],
                        "email" : client_row["EMAIL"]
                    }

                    if(contato not in client["listOutrosContatos"]):
                        client["listOutrosContatos"].append(contato)
            final_client_list.append(client)
    result = send_data_to_crm(CRM_USERS_URL, final_client_list)
    logging.info(datetime.now().strftime("%H:%M:%S - ") + result.json())


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
            negotiation = {
                "idExterno": first_organization_row["IDEXTERNO"],
                "nome": first_organization_row["NOME"],
                "contato": {
                    "idExterno": first_organization_row["IDEXTERNO_CONTATO"]
                },
                "organizacao": {
                    "idExterno": first_organization_row["IDEXTERNO_ORGANIZACAO"]
                    },
                "idEtapaNegociacao": 1,
                "statusNegociacao": first_organization_row["STATUSNEGOCIACAO"],
                "valor": first_organization_row["VALOR"],
                "observacoes": first_organization_row["OBSERVACOES"],
                "criadaEm": first_organization_row["CRIADAEM"],
                "listProduto": [],
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
                        "comentarios": negotiation_row["COMENTARIOS"]
                    }
                    if(negotiation_product_obj not in negotiation["listProduto"]):
                        negotiation["listProduto"].append(negotiation_product_obj)

            final_negotiation_list.append(negotiation)
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
        replace_contact_column_names(query_result)
    elif(sql_file == config['SQL_QUERIES']['negotiations']):
        replace_negotiation_column_names(query_result)

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
        st.configure(font='TkFixedFont')
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
    try:
        logging.info(datetime.now().strftime("%H:%M:%S - ") + "Bem-vindo ao Integrador Saga Sistemas")
        logging.info(datetime.now().strftime("%H:%M:%S - ") + "Validando login de cliente")
        if(validate_client_permission()):
            logging.info(datetime.now().strftime("%H:%M:%S - ") + "Login validado")
            logging.info(datetime.now().strftime("%H:%M:%S - ") + "Iniciando conexão com o banco Oracle")
            connection = database_connect()
            logging.info(datetime.now().strftime("%H:%M:%S - ") + "Conexão estabelecida")
            cur = connection.cursor()

            root = tk.Tk()
            myGUI(root)

            logging.info(datetime.now().strftime("%H:%M:%S - ") + "Iniciando sincronização de dados de clientes")
            synchronize_data(cur, config['SQL_QUERIES']['clients'], CRM_USERS_URL)
            logging.info(datetime.now().strftime("%H:%M:%S - ") + "Sincronização de clientes completa")
            logging.info(datetime.now().strftime("%H:%M:%S - ") + "Iniciando sincronização de dados de negociações")
            synchronize_data(cur, config['SQL_QUERIES']['negotiations'], CRM_NEGOTIATIONS_URL)
            logging.info(datetime.now().strftime("%H:%M:%S - ") + "Sincronização de negociações completa")

            connection.close()
            logging.info(datetime.now().strftime("%H:%M:%S - ") + "Conexão com o banco Oracle encerrada")
        else:
            logging.error("O cliente não possui permissão para executar a operação")
    except Exception as exception:
        print(type(exception).__name__)
        logging.info(datetime.now().strftime("%H:%M:%S - ") + "Erro de conexão, por favor reinicie a aplicação.")

if __name__ == "__main__":
    root = tk.Tk()
    myGUI(root)

    t1 = threading.Thread(target=worker, args=())
    t1.start()

    root.protocol("WM_DELETE_WINDOW", sys.exit)
    root.mainloop()
    t1.join()






    # schedule.every(int(config['SCHEDULE']['time_gap'])).minutes.do(test)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
