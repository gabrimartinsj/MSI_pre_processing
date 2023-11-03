import psycopg2

# Parâmetros de conexão ao banco de dados
db_params = {
    "host": "localhost",
    "database": "telegram2",
    "user": "postgres",
    "password": "Reve1945"
}

try:
    # Conectando ao banco de dados
    conn = psycopg2.connect(**db_params)

    # Criando um cursor
    cursor = conn.cursor()

    # Exemplo de consulta SQL
    cursor.execute("SELECT * FROM users")

    # Recuperando os resultados
    result = cursor.fetchall()
    for row in result:
        print(row)
        break;

    # Fechando o cursor
    cursor.close()

    # Fechando a conexão
    conn.close()

except (Exception, psycopg2.Error) as error:
    print("Erro ao conectar ao banco de dados:", error)
