import os
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("mysql://root:admin123@localhost:3306/projeto_marreta_f1")

list_arquivos = os.listdir("./Data")

for arquivo in list_arquivos:
    df = pd.read_csv(f"./Data/{arquivo}")
    nome_tabela = arquivo.replace(".csv", "")
    for coluna in df.columns:
        df = df.replace(to_replace=r"^\\N$", value="", regex=True)

    df.to_csv(f"./Data_Tratada/{arquivo}", header=True, sep=";", index=False)
    df.to_sql(nome_tabela, con=engine, if_exists="replace", index=False)

engine.dispose()
