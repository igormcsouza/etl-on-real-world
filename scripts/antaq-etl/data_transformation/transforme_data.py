"""Create the Required tables."""

from glob import glob
from typing import List
from logging import info, error

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from .utils import get_month, get_year


# Start a spark session
spark = SparkSession.builder.appName("Antaq Etl").getOrCreate()

# Create a main dictionary which will hold all the dataframes for later use
df = dict()
for year in glob("data/*"):
    df[year.split('/')[-1]] = dict()
    for dataset in glob(year + "/*.txt"):
        df[year.split('/')[-1]][dataset.split('/')[-1].split('.')[0]] = \
            spark.read.csv(dataset, sep=";", header=True)

def create_atracacao_fato(years: List[int]):
    """Create the 'atracacao_fato' table at data/."""
    info('Transforming data and creating "atracacao_fato" table')

    try:
        assert len(df.keys()) > 0, "There is no data on data folder"
    except AssertionError as ae:
        error("Could not transform data.", ae)
    else:
        for year in years:
            df[f"{year}"][f"{year}Atracacao"] = \
                df[f"{year}"][f"{year}Atracacao"]\
                    .select([
                        'IDAtracacao', 
                        'CDTUP',
                        'IDBerco',
                        'Berço',
                        'Porto Atracação',
                        'Apelido Instalação Portuária',
                        'Complexo Portuário',
                        'Tipo da Autoridade Portuária',
                        'Data Atracação',
                        'Data Chegada',
                        'Data Desatracação',
                        'Data Início Operação',
                        'Data Término Operação',
                        'Tipo de Operação'
                    ])

            df[f"{year}"][f"{year}Atracacao"] = \
                df[f"{year}"][f"{year}Atracacao"]\
                    .withColumn(
                        'Ano da data de início da operação', 
                        get_year(
                            df[f"{year}"][f"{year}Atracacao"][
                                'Data Início Operação']
                        )
                    )\
                    .withColumn(
                        'Mês da data de início da operação', 
                        get_month(
                            df[f"{year}"][f"{year}Atracacao"][
                                'Data Início Operação']
                        )
                    )

        atracacao_fato = \
            df["2018"]["2018Atracacao"].union(
                df["2019"]["2019Atracacao"].union(
                    df["2020"]["2020Atracacao"]
                )
            )

        try:
            atracacao_fato.write.csv('data/atracacao_fato.csv', header=True)
        except AnalysisException as ae:
            error('Table not saved.', ae)

def create_carga_fato(years: List[int]):
    """Create the 'carga_fato' table at data/."""
    info('Transforming data and creating "carga_fato" table')

    try:
        assert len(df.keys()) > 0, "There is no data on data folder"
    except AssertionError as ae:
        error("Could not transform data.", ae)
    else:
        for year in years:
            df[f"{year}"][f"{year}Carga"] = df[f"{year}"][f"{year}Carga"]\
                .select([
                    'IDCarga',
                    'IDAtracacao',
                    'Origem',
                    'Destino',
                    'CDMercadoria',
                    'Tipo Operação da Carga',
                    'Carga Geral Acondicionamento',
                    'ConteinerEstado',
                    'Tipo Navegação',
                    'FlagAutorizacao',
                    'FlagCabotagem',
                    'FlagCabotagemMovimentacao',
                    'FlagConteinerTamanho',
                    'FlagLongoCurso',
                    'FlagMCOperacaoCarga',
                    'FlagOffshore'
                ])
        
        carga_fato = \
            df["2018"]["2018Carga"].union(
                df["2019"]["2019Carga"].union(
                    df["2020"]["2020Carga"]
                )
            )

        try:
            carga_fato.write.csv('data/carga_fato.csv', header=True)
        except AnalysisException as ae:
            error('Table not saved.', ae)