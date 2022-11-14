# Projeto de ingestão e análise de dados de vôos
[![en](https://img.shields.io/badge/lang-en-red.svg)](https://github.com/gpeixinho/flights-project/blob/master/README.md)

:warning: Em desenvolvimento :warning:	

## Proposta

Estruturar um data lake contendo dados de ofertas de voos coletados automaticamente de sites de agências de viagens, dados históricos abertos de voos, informações sobre aeroportos e linhas áreas para análise de ofertas de voos que foram canceladas pelas companhias aéreas (não existem mais).

## Motivação

Tenho ouvido falar recentemente de casos de clientes que compraram passagens online em agências de viagens, e quando estavam prestes a fazer o check-in perceberam que o voo simplesmente não existia, pode ter existido antes, mas já faz algum tempo que não é realizado pela companhia aérea. Na época, suspeitei que isso fosse um problema de integração de dados e decidi construir este projeto para verificar se os voos oferecidos aconteceram nas últimas semanas ou meses.

## Resultados esperados

O projeto proverá os dados necessários para verificação de ofertas de agências, possivelmente encontrando ofertas de voos que não são mais realizados por companhias aéreas. Encontrar informações sobre históricos de voos pode ser complexo, sendo que as fontes não possuem cobertura completa, uma taxa alta de falsos positivos é esperada, no início está previsto somente uma fonte de dados históricos de vôos (https://opensky-network.org/), sendo novas fontes (por exemplo de agências nacionais de aviação) acrescentadas com o tempo para que a análise fique cada vez mais completa.
O data lake poderá ser estendido ainda com mais fontes de informação para futuros estudos sobre tráfego aéreo e também sobre ofertas de voos. 

## Métodos

Serão utilizados scripts python para a coleta dos dados das fontes especificadas, a implantação destes scripts será feita em nuvem por meio de funções AWS Lambda, os dados brutos serão armazenados no formato .json e .csv no bucket "raw" do data lake, estes dados serão carregados no Spark para transformação para forma tabular e finalmente serem gravados no formato .parquet no bucket processed do data lake. Análises simples como a verificação da ocorrência dos voos nos últimos meses já podem ser realizadas com os dados nesta forma por meio do AWS Athena. Futuramente podem ser construídas métricas que serão servidas na camada "curated" do data lake.

## Objetivo

Os objetivos principais são obter familiaridade e vivência nos processos e ferramentas de engenharia de dados utilizados no mercado bem como demonstrar habilidades nestes, dito isto, entende-se que o processamento dos dados não demanda necessariamente os serviços selecionados.

## Fontes de dados 

- Ofertas são obtidas por de web crawlers que consultam APIs de agências
- Dados de aeroportos e companhias relevantes para análise são obtidos por "scraping" das respectivas páginas da Wikipedia
- Dados de voos são obtidos por meio da API aberta do Opensky (https://opensky-network.org/)

## Ferramentas

- AWS Lambda para implatação dos scripts de ingestão
- AWS S3 armazenamento dos dados
- AWS Athena para consultas simples
- Apache Spark para processamento dos dados (provisionamento via Databricks)
- Airflow para orquestração de todos os jobs

## Arquitetura  

  


![Pipeline Diagram](imgs\diagram-pt-br_20221113.jpg)