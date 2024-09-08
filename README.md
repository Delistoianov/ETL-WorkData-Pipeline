# ETL-WorkData-Pipeline

Para executar o processo de transformação dos dados raw em dados work: 

1. Na root do projeto executar o comando 'docker compose up --build', para subir a imagem do clickhouse
2. Usar algum gerenciador de banco de dados para se conectar ao clickhouse. Estou usando o Dbeaver.
3. Carregar arquivos em formato csv para a pasta tmp. Deixei 3 CSVs de exemplo dentro da pasta para facilitar os testes
4. Na root, executar o comando 'python .\main.py'