import psycopg2
import re
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# Parâmetros de conexão ao banco de dados
db_params = {
    "host": "localhost",
    "database": "telegram2",
    "user": "postgres",
    "password": "Reve1945"
}

url_pattern = r'(https?://\S+)'

try:
    # Conectando ao banco de dados
    conn = psycopg2.connect(**db_params)

    # Criando um cursor
    cursor = conn.cursor()

    # Exemplo de consulta SQL com paginação
    page_size = 100000  # Defina o tamanho da página desejado
    offset = 0

    total_messages_captured = 0  # Variável para contagem de mensagens capturadas

    while True:
        cursor.execute("SELECT * FROM messages_order_by_channel_utc LIMIT %s OFFSET %s", (page_size, offset))
        results = cursor.fetchall()

        if not results:
            break

        # Crie uma lista para armazenar as linhas capturadas na página atual
        new_rows = []

        for row in results:
            if re.findall(url_pattern, row[3]):
                # Adicione a linha capturada à lista de linhas da página atual
                new_rows.append([row[2], row[3], row[4]])

                # Calcule as faixas de tempo para considerar
                start_time = row[4] - timedelta(minutes = 1)
                end_time = row[4] + timedelta(minutes = 3)

                for i in range(results.index(row) - 1, -1, -1):
                    if row[2] == results[i][2] and start_time <= results[i][4]:
                        new_rows.append([results[i][2], results[i][3], results[i][4]])
                    else:
                        break

                for i in range(results.index(row) + 1, len(results)):
                    if row[2] == results[i][2] and results[i][4] <= end_time:
                        new_rows.append([results[i][2], results[i][3], results[i][4]])
                    else:
                        break

        # Atualize a contagem de mensagens capturadas
        total_messages_captured += len(new_rows)
        print(f"Número de mensagens já capturadas: {total_messages_captured}")

        # Crie um DataFrame a partir da lista de linhas da página atual
        df = pd.DataFrame(new_rows, columns = ["channel_id", "message_data", "message_utc"])
        # Remova as duplicatas do DataFrame, se necessário
        df = df.drop_duplicates()

        # Conecte-se novamente para inserir os dados na tabela do PostgreSQL
        engine = create_engine('postgresql://postgres:Reve1945@localhost:5432/telegram2')
        df.to_sql('messages_filtered_by_context_window_by_time_v6', engine, if_exists = 'append', index = False)

        offset += page_size

    print("\n")
    print('Fim das inserções.')

    # Fechando o cursor
    cursor.close()

    # Fechando a conexão
    conn.close()

except (Exception, psycopg2.Error) as error:
    print("Error:", error)
