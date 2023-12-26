import psycopg2
import csv

# Parâmetros de conexão ao banco de dados
db_params = {
    "host": "localhost",
    "database": "telegram2",
    "user": "postgres",
    "password": "Reve1945"
}

# Nome do arquivo CSV de saída
output_csv = 'dataset_video_platform_links_all_reprocessed_1.csv'

try:
    # Conectando ao banco de dados
    conn = psycopg2.connect(**db_params)

    # Criando um cursor
    cursor = conn.cursor()

    # Exemplo de consulta SQL com paginação
    page_size = 100000  # Defina o tamanho da página desejado
    offset = 0

    total_messages_captured = 0  # Variável para contagem de mensagens capturadas

    # Abre o arquivo CSV para escrita
    with open(output_csv, 'w', newline = '', encoding = 'utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        # Escreve o cabeçalho no arquivo CSV
        csv_writer.writerow(["channel_id", "author_id", "message_data", "message_utc", "window_id"])

        while True:
            cursor.execute("SELECT * FROM messages_filtered_by_context_window_by_time_v1_4 LIMIT %s OFFSET %s", (page_size, offset))
            results = cursor.fetchall()

            if not results:
                break

            # Escreve as linhas diretamente no arquivo CSV
            for row in results:
                csv_writer.writerow(row)

            # Atualiza a contagem de mensagens capturadas
            total_messages_captured += len(results)
            print(f"Número de mensagens capturadas: {total_messages_captured}")

            offset += page_size

    print("\n")
    print('Fim da exportação para CSV.')

    # Fechando o cursor
    cursor.close()

    # Fechando a conexão
    conn.close()

except (Exception, psycopg2.Error) as error:
    print("Erro na exportação para CSV:", error)
