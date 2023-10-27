import os
import pandas as pd

list_arquivos = os.listdir('./Data')

for arquivo in list_arquivos:
    df = pd.read_csv(f'./Data/{arquivo}')
    for coluna in df.columns:
        df = df.replace(to_replace=r'^\\N$', value='', regex=True)
    
    df.to_csv(f'./Data_Tratada/{arquivo}', header=True, sep=';', index=False)