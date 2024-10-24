# Projeto Técnico de Engenharia de Dados: Cobli


## Objetivo
O objetivo deste projeto é criar um pipeline de dados de ponta a ponta a partir de um conjunto de dados forneciido pela empresa Cobli como projeto técnico da posição de engenharia de dados pleno. Isso envolve modelar os dados em tabelas de fato e dimensão, implementar etapas de qualidade de dados, utilizar tecnologias modernas de dados (dbt,  Airflow e BigQuery) e armazenar os dados na nuvem (Google Cloud Platform). O projeto é containerizado via Docker e versionado no GitHub.

**Tecnologias Utilizadas**
- Python
- Docker e Docker-compose
- Google Cloud Storage
- Google BigQuery
- Airflow (versão Astronomer)
- dbt
- GitHub (Repositório AQUI)

Na pasta ```/include/datasets/``` você encontrará o arquivo: o ```acidentes_brasil.csv```, fornecido pelo time técnico da Cobli para deste projeto.

---------
## Para executar este projeto, você deve

### Instalar o Docker
[Instale o Docker para o seu sistema operacional](https://docs.docker.com/desktop/)

### Instalar o Astro CLI
[Instale o Astro CLI para o seu sistema operacional](https://www.astronomer.io/docs/astro/cli/install-cli)

### Clonar o repositório do GitHub

No seu terminal:

Clone o repositório usando o GitHub CLI ou Git CLI
```bash
gh repo clone mferreiracosta/cobli-mid-data-engineer-case
```

ou

```bash
git clone https://github.com/mferreiracosta/cobli-mid-data-engineer-case.git
```

Abra a pasta com seu editor de código.

### Reinitialize the Airflow project

Abra o terminal do editor de código:

```bash
astro dev init
```
Irá perguntar: ```Você não está em um diretório vazio. Tem certeza de que deseja inicializar um projeto? (y/n)```
Digite ```y``` e o projeto será reiniciado.

### Construir o projeto
No terminal do editor de código, digite:

```bash
astro dev start
```
O endpoint padrão do Airflow é o http://localhost:8080/
- username: admin
- password: admin


### Criar o projeto GCP

No seu navegador, vá para https://console.cloud.google.com/ e crie um projeto, recomendado algo como: ```cobli-mid-data-engineer-case```

Copie seu ID do projeto e salve-o para mais tarde.

#### Usando o ID do projeto do GCP

Altere os seguintes arquivos:
- include\dbt\models\sources.yml (database)
- include\dbt\profiles.yml (project)

#### Criar um Bucket no GCP

Com o projeto selecionado, vá para https://console.cloud.google.com/storage/browser e crie 2 Buckets.
Use os nomes ```plataforma-dados-cobli-silver``` e ```plataforma-dados-cobli-bronze```.

#### Criar uma conta de serviço para o projeto

Vá para a guia IAM e crie a conta de serviço com o nome ```airflow-acidentes-brasil```.
Dê acesso de administrador ao GCS e BigQuery, e exporte as chaves json. Renomeie o arquivo para service_account.json e coloque dentro da pasta ```include/gcp/``` (você terá que criar essa pasta).

#### Criar uma conexão no seu Airflow

No seu Airflow, em http://localhost:8080/, faça login e vá para Admin → Connections.
Crie uma nova conexão e use as seguintes configurações:
- conn_id: google_cloud_default
- conn_type: Google Cloud
- project_id: cobli-mid-data-engineer-case
- keyfile_path `/usr/local/airflow/include/gcp/service-account.json`

Teste e salve.

### Tudo pronto, inicie o DAG

Com o seu Airflow em execução, vá para http://localhost:8080/ e clique em DAGs, e ative as DAGs ```ingestion_gcp_pipeline``` e ```consumer_bigquery_dbt_datasets```.
Em seguida, inicie o DAG de ingestão gcp(botão de play no canto superior direito).

Ele irá executar passo a passo, e se tudo for seguido corretamente, você obterá uma execução verde no final.
Verifique na sua conta do GCP Storage se o arquivo foi enviado com sucesso, na sua aba do BigQuery se as tabelas foram construídas.
