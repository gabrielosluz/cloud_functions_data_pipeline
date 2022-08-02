# Projeto: data pipeline com cloud fucntions

## Componentes GCP

Nesta seção apresentamos os componentes da Google Cloud Platform (GCP) presentes no projeto bem como o papel que cada um desempenha.

- **Google Cloud Scheduler**: Responsavel pelo agendamento de tarefas dentro da plataforma. No contexto do projeto é usado para agendar as queries que populam os Data Marts a partir do Data Warehouse. 

- **Bigquery Tables e Datasets**: O BigQuery é a ferramenta de Data Warehouse da GCP. Dentro nela, existe o conceito de Datasets, que atuam como agrupadores de tabelas que por sua vez contem os dados em si.

- **Cloud Storage**: Essa é a ferramenta da GCP que atua como repositório de dados semi-estruturados. No contexto deste projeto dois buckets do CLoud Storage são usados como etapas intermidiárias para a ingestão e tratamento inicial dos dados. 

- **Cloud Functions**: Esse é um produto da GCP para realizar tarefas de computação de forma que não seja necessário provisionar e gerenciar nenhuma infraestrutura. No projeto, três Cloud Functions são usadas para desempenhar tarefas relacionadas ao processamento de dados. 

- **Cloud Source Repository**: Repositórios git para armazenar os arquivos do projeto.

- **Cloud build**: Ferramenta CI/CD da GCP. No contexto deste projeto é utilizada para tornar o deploy das Cloud Functions de forma automática a partir de commits e pushs no repositórios no Cloud Source Repository.


## Controle de ingestão

A Cloud Function **cf_convert_json_to_parquet** também executa uma importante etapa no processo de pipeline de dados, que é o preenchimento da tabela de controle. O objetivo desse passo é ter um controle de ingestão de arquivos. Com isso, para cada arquivo inserido, seja no contexto de emissão, sinistro ou cancelamento, a function **cf_convert_json_to_parquet** irá inserir na tabela de controle metadados sobre o arquivo processado. Dessa forma, ganhamos uma visão do processo e uma forma de analisar arquivos que foram ou não processados, fator que ajuda a localização de possíveis problemas no pipeline de dados em caso de falhas. 

## Arquivos do repositório

##### **Raiz do projeto**
| Path/Arquivo       | Objetivo                                                     |
| ------------------ | ------------------------------------------------------------ |
| README.md          | Contém a documentação basica.                                |
| .gitignore  | arquivo que especifica arquivos que não devem ser rastreados pelo git |

##### **/cf_convert_json_to_parquet**
| Path/Arquivo       | Objetivo                                                     |
| ------------------ | ------------------------------------------------------------ |
| /parametros     | Contém o arquivo Pyhton que define a classe de passagem de parametros para o arquivo principal da Cloud Function|
| cloudbuild.yaml               | Contém o arquivo que realiza o deploy automatizado na Cloud Function em questão |
| requirements.txt       | Contém todas as bibliotecas Python que são usadas pela Cloud Function           |
| main.py  | Contém o código Python 3.9 utilizado pela Cloud Function **cf_convert_json_to_parquet**, responsável por transformar os arquivos json em parquet|

##### **/cf_load_parquet_to_bq**
| Path/Arquivo       | Objetivo                                                     |
| ------------------ | ------------------------------------------------------------ |
| /parametros     | Contém o arquivo Pyhton que define a classe de passagem de parametros para o arquivo principal da Cloud Function|
| cloudbuild.yaml               | Contém o arquivo que realiza o deploy automatizado na Cloud Function em questão |
| requirements.txt       | Contém todas as bibliotecas Python que são usadas pela Cloud Function           |
| main.py  | Contém o código Python 3.9 utilizado pela Cloud Function **cf_load_parquet_to_bq**, responsável carregar os arquivos parquet para as suas respectivas tabelas no BigQuery|

## Processo de CI/CD

CI/CD designa respectivamente Continuous Integration e Continuous Delivery traduzindo: Integração Contínua e Entrega Contínua. Ambas as siglas designam processos e técnicas modernas para tornar o processo de desenvolvimento, teste e entrega de ferramentas mais ágil e eficiente. Para este projeto, foi usada a ferramenta Cloud Build. Ela permite que a partir de um commit e um push nos repositórios git do projeto (Cloud Source Repository), as mudanças nos códigos sejam imediatamente deployadas nas suas respectivas Cloud Funtions.

Para o projeto, os Triggers abaixo foram criados no Cloud Build. Cada um deles possui uma distinção quanto ao ambiente em que farão o deploy, baseado na branch. Os Triggers que possuem sufixo com -dev fazem em deploy em qualquer branch git que não seja a main ou a develop, para essas branchs específicas, os Triggers com o sufixo -develop foram criados. 
