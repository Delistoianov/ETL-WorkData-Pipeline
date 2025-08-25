# ETL WorkData Pipeline

Este projeto implementa um pipeline de ETL (Extração, Transformação e Carga) para processar dados brutos e carregá-los em um ambiente de análise.

## 🎯 Objetivo

O objetivo principal deste projeto é automatizar o processo de coleta, tratamento e armazenamento de dados de diversas fontes. A finalidade é transformar dados brutos em um formato limpo, padronizado e pronto para análise, servindo como base para dashboards, relatórios de business intelligence e modelos de data science.

## ⚙️ Fluxo Funcional

1.  **Ingestão de Dados**: O processo inicia com a disponibilização de arquivos de dados brutos (em formato CSV) em uma pasta de entrada designada (`/tmp`).
2.  **Processamento e Transformação**: O sistema converte os arquivos para um formato otimizado para análise (Parquet) e inicia o processo de transformação. Nesta etapa, os dados são limpos, valores ausentes são tratados, formatos são padronizados e informações inconsistentes são corrigidas.
3.  **Armazenamento**: Após a transformação, os dados processados e enriquecidos são carregados em um banco de dados central (ClickHouse), onde ficam disponíveis para consulta e análise.

## 🚀 Como Executar

Siga os passos abaixo para executar o pipeline de ETL:

1.  **Iniciar o Banco de Dados**
    Na raiz do projeto, execute o comando abaixo para iniciar o container do ClickHouse com Docker.
    ```bash
    docker-compose up --build
    ```

2.  **Adicionar Dados Brutos**
    Coloque os arquivos CSV que deseja processar na pasta `tmp/`. Arquivos de exemplo já estão disponíveis no diretório para facilitar os testes.

3.  **Executar o Pipeline**
    Na raiz do projeto, execute o comando a seguir para iniciar o processo de ETL.
    ```bash
    python main.py
    ```

4.  **Acessar os Dados**
    Utilize um gerenciador de banco de dados de sua preferência (como DBeaver, por exemplo) para se conectar ao ClickHouse na porta `8123` e visualizar os dados processados.