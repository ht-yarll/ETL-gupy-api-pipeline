import pathlib
from api.gupy import gupy_fetch_data
from modules.DataProcessor import DataProcessor
import pandas as pd

working_dir = pathlib.Path.cwd()
files_folder = working_dir.joinpath('data/files_from_script')
processor = DataProcessor(files_folder)
df_jobs = gupy_fetch_data()

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

def check_state(state_name):
    state_abbreviations = {
        "acre": "AC",
        "alagoas": "AL",
        "amapá": "AP",
        "amazonas": "AM",
        "bahia": "BA",
        "ceará": "CE",
        "distrito federal": "DF",
        "espírito santo": "ES",
        "goiás": "GO",
        "maranhão": "MA",
        "mato grosso": "MT",
        "mato grosso do sul": "MS",
        "minas gerais": "MG",
        "pará": "PA",
        "paraíba": "PB",
        "paraná": "PR",
        "pernambuco": "PE",
        "piauí": "PI",
        "rio de janeiro": "RJ",
        "rio grande do norte": "RN",
        "rio grande do sul": "RS",
        "rondônia": "RO",
        "roraima": "RR",
        "santa catarina": "SC",
        "são paulo": "SP",
        "sergipe": "SE",
        "tocantins": "TO"
    }

    state_name_lower = state_name.lower()

    for full_name, abbreviation in state_abbreviations.items():
        if full_name in state_name_lower:
            return abbreviation

    return "STATE NOT RECOGNIZED"

df_jobs.insert(
    loc = 14,
    columns = 'state_abbreviation',
    value = df_jobs['state'].apply(check_state)
)


df_jobs['type'] = df_jobs['type'].replace(
    {
    'vacancy_type_effective': 'Efetivo',
    'vacancy_type_internship': 'Estágio',
    'vacancy_type_talent_pool': 'Banco de Talentos',
    'vacancy_type_temporary': 'Temporário',
    'vacancy_type_apprentice': 'Aprendiz',
    'vacancy_type_freelancer': 'Freelancer',
    'vacancy_legal_entity': 'PJ',
    'vacancy_type_associate': 'Associado',
    'vacancy_type_autonomous': 'Autonomo',
    'vacancy_type_lecturer': 'Professor'
    }
)

def data_treated() -> None:
    df = processor.treat_data(df_jobs)
    df_parquet = processor.save_to_parquet(df, file_name='gupy_data.parquet')
    return df_parquet