# ETL_1bilhao

## Gerar dados

Para gerar os dados utilizados como entrada, é necessário executar o script **create_measurements.py**.

Esse script irá gerar os dados aleatórios de medidas de temperaturas no diretório **data**.

Está configurado para gerar 1000000000 (1 bilhão) de linhas. Isso pode ser alterado no arquivo do script.

Com a configuração atual, o arquivo gerado terá aproximadamente  16,5 GB.

## Executar ETLs

Para executar as diferentes versões do ETL, é necessário executar o script correspondente à versão desejada.

Os scripts de ETL seguem o padrão de nome **etl_*versao*.py**.

## Gerar arquivo Parquet

Para executar o dashboard, é necessário gerar o arquivo Parquet através do script **duckdb_to_parquet.py**.

Esse script irá gerar o arquivo Parquet no diretório **data**.

## Dashboard

Para mostrar o dashboard, é necessário executar o script **dash_duckdb_parquet.py**, do diretório *dashboard*.

O Streamlit irá abrir uma aba no navegador apresentando o dashboard.