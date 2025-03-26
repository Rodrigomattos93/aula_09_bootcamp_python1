import pandas as pd
import glob
import os
from decorador import log_decorator

@log_decorator
def transformar_jsons_em_dataframe_concatenado() -> pd.DataFrame:
    pasta: str = input("Indique o nome da pasta que você deseja obter os arquivos json: ")
    arquivos_json: list = glob.glob(os.path.join(pasta, '*.json'))
    arquivo_json_final: pd.DataFrame = pd.DataFrame()

    for arquivo in arquivos_json:
        reader: pd.DataFrame = pd.read_json(arquivo)
        arquivo_json_final = pd.concat([arquivo_json_final, reader], ignore_index=True)
    
    return arquivo_json_final

@log_decorator
def criar_coluna_faturamento_em_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df['Valor Total'] = df['Quantidade'] * df['Venda']
    return df

@log_decorator
def gerar_arquivos_de_saida(df2: pd.DataFrame):
    formato: list = list(
        input("Digite csv e/ou parquet para definir o formato de saída: ").replace(" ", "").split(','))
    if 'csv' in formato:
        df2.to_csv('projeto_aula08_saida.csv')
    if 'parquet' in formato:
        df2.to_parquet('projeto_aula08_saida.parquet')
    return

saida_funcao1 = transformar_jsons_em_dataframe_concatenado()
saida_funcao2 = criar_coluna_faturamento_em_dataframe(saida_funcao1)
saida_funcao3 = gerar_arquivos_de_saida(saida_funcao2)