from api.gupy import gupy_fetch_data
from modules.DataProcessor import DataProcessor
import pandas as pd

df_jobs = gupy_fetch_data()
processor = DataProcessor()

# Creating job_type column
a_dados = ['dados']
a_uxui = ['uxui', 'ux ui', 'ux/ui']
a_adm = ['adm', 'administração', 'administracao']
a_rh = ['rh', 'recursos humanos']
a_frtend = ['frontend']

def check_category(x):
    if any(keyword  in x.lower() for keyword in a_dados):
        return 'DADOS'
    elif any(keyword  in x.lower() for keyword in a_uxui):
        return 'UX/UI'
    elif any(keyword  in x.lower() for keyword in a_adm):
        return 'ADM'
    elif any(keyword  in x.lower() for keyword in a_rh):
        return 'RH'
    elif any(keyword  in x.lower() for keyword in a_frtend):
        return 'FRTEND'
    else:
        return 'DADO NÃO IDENTIFICADO'

df_jobs['job_type'] = df_jobs['name'].apply(check_category)

# Creating exprience column
a_intern = ['estagiário', 'estágio', 'estagio', 'estagiario']
a_junior = ['junior', 'jr', 'júnior']
a_basic = ['pleno', 'pl']
a_senior = ['senior', 'sr', 'sênior']

def check_experience(x):
    if any(keyword in x.lower() for keyword in a_intern):
        return 'ESTAGIO'
    elif any(keyword in x.lower() for keyword in a_junior):
        return 'JUNIOR'
    elif any(keyword in x.lower() for keyword in a_basic):
        return 'PLENO'
    elif any(keyword in x.lower() for keyword in a_senior):
        return 'SENIOR'
    else:
        return 'DADO NÃO IDENTIFICADO'
    
df_jobs['experience'] = df_jobs['name'].apply(check_experience)

def data_treated() -> None:
    df = processor.treat_data(df_jobs)
    df_parquet = processor.save_to_parquet(df, file_name='gupy_data.parquet')
    return df_parquet