# FIEC Seleção - Cientista de Dados

[PROVA PRÁTICA] SELEÇÃO 54/2021 - CIENTISTA DEDADOS - FOCO EM ENGENHARIA DE DADOS

Os documentos para o projeto estão descritos na pasta [about](about/)

## Como iniciar o projeto?

As dependencias estão descritas no arquivo [requirements.txt](requirements.txt),
inicie um ambiente virtual python e instale as dependencias.

```bash
root$ virtualenv .venv
root$ pip install -r requirements.txt
```

### Aplicação da questão 2

Para iniciar a aplicação basta executar o arquivo
[main.py](scripts/antaq-etl/main.py), os arquivos serão baixados e transformados
automaticamente.

```bash
root$ python scripts/antaq-etl/main.py
```

> _Nota_: A pasta [data](data/) está na raiz do projeto, então executar comandos
> fora da raiz podem ocasionar em erros de localização de arquivos.

## Utilizar Apache Airflow

Airflow está configurado para ser usado via Docker.
