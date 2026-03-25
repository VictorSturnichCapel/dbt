# 🎓 Data Warehouse Educacional: ENEM & CENSO
### Projeto dbt: `dbt_dw_victor`

Este repositório contém o projeto **dbt** (data build tool) para a modelagem e transformação dos microdados da educação brasileira, integrando os resultados do **ENEM** com os dados estruturais do **Censo Escolar** com informações escolares.

---

## 🏗️ Arquitetura de Dados (Medalhão)

O projeto segue as melhores práticas de Analytics Engineering, dividindo as transformações em camadas para garantir a linhagem e a qualidade do dado:

* **Staging (`models/staging/`)**: Camada de entrada. Realizamos o `SAFE_CAST` dos tipos de dados, renomeação de colunas para inglês (padronização técnica) e limpezas superficiais. 
    * *Exemplo:* `stg_enem__dados_2024`.
* **Intermediate (`models/intermediate/`)**: Camada de pré-processamento. Aqui ocorrem os joins entre anos diferentes ou cruzamentos complexos que não são a entrega final, mas facilitam o cálculo de métricas.
* **Marts / Business (`models/marts/`)**: Camada de consumo (Gold). Contém as tabelas prontas para o BI.
    * `fct_`: Tabelas Fato (ex: desempenho dos alunos, métricas de notas).
    * `dim_`: Tabelas Dimensão (ex: dados das escolas, localização, perfil socioeconômico).

---

## 🛠️ Stack Tecnológica
* **Transformação:** [dbt Core](https://docs.getdbt.com/)
* **Warehouse:** BigQuery (Google Cloud)
* **Linguagem:** SQL (Padrão GoogleSQL)

---

## 🚀 Como Desenvolver

### 1. Instalação de Dependências
Caso o projeto utilize pacotes externos (como `dbt_utils`), execute:
```bash
dbt deps
