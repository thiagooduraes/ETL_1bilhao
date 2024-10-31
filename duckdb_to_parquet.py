import duckdb
import time

def create_duckdb():
    result = duckdb.sql("""
    SELECT
        cidade,
        min(temperatura) as temp_min,
        avg(temperatura) as temp_med,
        max(temperatura) as temp_max
    FROM read_csv("data/measurements.txt", AUTO_DETECT=FALSE, sep=';', columns={'cidade':VARCHAR, 'temperatura':'DECIMAL(3,1)'})
    GROUP BY cidade
    ORDER BY cidade
    """)

    result.show()

    #Salvando em arquivo parquet
    result.write_parquet('data/measurements_summary.parquet')


if __name__ == "__main__":
    import time

    print("Iniciando o processamento do arquivo.")
    start_time = time.time()
    create_duckdb()
    took = time.time() - start_time
    
    print(f"Processamento levou {took:.2f} segundos.")