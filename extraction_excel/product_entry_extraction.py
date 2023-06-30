import pandas as pd

dados_excel = pd.read_excel('C:\\Users\\conta\\colibri_data\\colibri.xlsx', sheet_name=None)
#coluna_a = dados_excel['Coluna A']


for nome_planilha, dados_planilha in dados_excel.items():
    # Realizar as operações desejadas com cada planilha
    print("Nome da planilha:", nome_planilha)
    print("Dados da planilha:")
    dados_planilha
    print(dados_planilha)
    print("\n")