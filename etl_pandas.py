import pandas as pd
from multiprocessing import Pool, cpu_count
from tqdm import tqdm # Barra de progresso

CONCURRENCY = cpu_count()

total_linhas = 1_000_000_000 # Total de linhas
chunksize = 100_000_000 # Tamanho do chunk
filename = "data\\measurements.txt"

def process_chunk(chunk):
    #Agrega os dados no chunk com o Pandas
    aggregated = chunk.groupby('station')['measure'].agg(['min', 'max', 'mean']).reset_index()
    return aggregated

def create_df_with_pandas(filename, total_linhas, chunksize=chunksize):
    total_chunks = total_linhas // chunksize + (1 if total_linhas % chunksize else 0)
    results = []
    print(CONCURRENCY)
    with pd.read_csv(filename, sep=';', header=None, names=['station','measure'], chunksize=chunksize) as reader:
        # TQDM para visualizar progresso
        with Pool(CONCURRENCY) as pool:
            # For Paralelo
            for chunk in tqdm(reader, total=total_chunks, desc="Processando..."):
               result = pool.apply_async(process_chunk, (chunk,))
               results.append(result)

            results = [result.get() for result in results]

        
    final_df = pd.concat(results, ignore_index=True)

    final_aggregated_df = final_df.groupby('station').agg({
        'min': 'min',
        'max': 'max',
        'mean': 'mean'
    }).reset_index().sort_values('station')

    return final_aggregated_df

if __name__ == "__main__":
    import time

    print("Iniciando o processamento do arquivo.")
    start_time = time.time()
    df = create_df_with_pandas(filename, total_linhas, chunksize)
    took = time.time() - start_time

    print(df.head())
    print(f"Processamento levou {took:.2f} segundos.")