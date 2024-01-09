import psycopg2
import schedule
import time

connection_config = psycopg2.connect(
    host="177.220.159.198",
    database="Esteio",
    user="postgres",
    password="cadastro"
)

def update_database():
    file_name = r"C:\Users\0519\Documents\Padronizacao_Banco_Dados\updateGeral.sql"
    commands = []

    connection = connection_config.cursor()

    with open(file_name, "r", encoding="utf-8") as file:
        content = file.read()

    commands = content.split(';')
    for command in commands:
        try:
            print(command)
            connection.execute(command)
            connection_config.commit()
        except Exception as e:
            print(f"Erro ao executar o comando: {command}\nErro: {e}")
            connection_config.rollback()
    connection.close()
    connection_config.close() 

schedule.every(3).hours.do(update_database)
while True:
    schedule.run_pending()
    time.sleep(3600)