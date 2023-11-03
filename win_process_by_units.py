import psycopg2
import re
import pandas as pd
from sqlalchemy import create_engine

# Parâmetros de conexão ao banco de dados
db_params = {
    "host": "localhost",
    "database": "telegram2",
    "user": "postgres",
    "password": "Reve1945"
}

url_pattern = r'(https?://\S+)'

# Crie uma lista para armazenar as linhas capturadas
new_rows = []

try:
    # Conectando ao banco de dados
    conn = psycopg2.connect(**db_params)

    # Criando um cursor
    cursor = conn.cursor()

    # Exemplo de consulta SQL com paginação
    page_size = 100  # Defina o tamanho da página desejado
    offset = 0

    while True:
        cursor.execute("SELECT * FROM messages_order_by_channel_utc LIMIT %s OFFSET %s", (page_size, offset))
        results = cursor.fetchall()

        if not results:
            break

        for row in results:
            if re.findall(url_pattern, row[3]):
                # Imprime o conjunto encontrado
                print("Mensagem com URL:")
                print("\n")
                print(row[2])
                print(row[3])
                print(row[4])
                print("\n")

                # Adicione a linha capturada à lista de linhas
                new_rows.append([row[2], row[3], row[4]])

                # Imprime as 5 linhas anteriores
                print("5 linhas anteriores:")
                print("\n")
                for i in range(max(0, results.index(row) - 5), results.index(row)):
                    if results[i][2] == row[2]:
                        print(results[i][2])
                        print(results[i][3])
                        print(results[i][4])
                        print("\n")

                        # Adicione a linha capturada à lista de linhas
                        new_rows.append([results[i][2], results[i][3], results[i][4]])
                print("\n")

                # Imprime as 20 linhas seguintes
                print("20 linhas seguintes:")
                print("\n")
                for i in range(results.index(row) + 1, min(results.index(row) + 21, len(results))):
                    if results[i][2] == row[2]:
                        print(results[i][2])
                        print(results[i][3])
                        print(results[i][4])
                        print("\n")

                        # Adicione a linha capturada à lista de linhas
                        new_rows.append([results[i][2], results[i][3], results[i][4]])
                print("\n")

        offset += page_size

    # Crie um DataFrame a partir da lista de linhas
    df = pd.DataFrame(new_rows, columns = ["channel_id", "message_data", "message_utc"])

    # Remova as duplicatas do DataFrame, se necessário
    df = df.drop_duplicates()

    print("Dataframe capturado:")
    print("\n")
    print(df)

    # Conecte-se novamente para inserir os dados na tabela do PostgreSQL
    engine = create_engine('postgresql://postgres:Reve1945@localhost:5432/telegram2')
    df.to_sql('messages_filtered_by_context_window', engine, if_exists = 'append', index = True)

    # Fechando o cursor
    cursor.close()

    # Fechando a conexão
    conn.close()

except (Exception, psycopg2.Error) as error:
    print("Error:", error)
