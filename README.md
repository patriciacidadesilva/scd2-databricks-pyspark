# 📊 SCD Type 2 com Databricks e PySpark  


## 📌 Visão Geral
Este projeto apresenta uma implementação **completa e correta de SCD Type 2 (Slowly Changing Dimension)** utilizando **PySpark e Delta Lake no Databricks**.

O foco é demonstrar **versionamento histórico de atributos ao longo do tempo**, com **detecção de mudanças**, **fechamento de vigência**, **inserção de novas versões** e **idempotência**, seguindo padrões amplamente adotados em arquiteturas Lakehouse.

> ⚠️ **Nota importante:**  
> Todos os dados, tabelas, nomes e regras de negócio são **fictícios** e existem exclusivamente para **fins educacionais e de portfólio técnico**.

---

## 🎯 Problema de Negócio (Contexto Fictício)
Em domínios financeiros e analíticos, atributos como **nível de risco** evoluem ao longo do tempo.  
Sobrescrever valores elimina a capacidade de responder perguntas críticas como:

- Qual era o risco desta conta em um período específico?
- Quando ocorreu uma mudança de status?
- Por quanto tempo uma conta permaneceu em determinado nível?

Essas perguntas exigem **histórico versionado**, não apenas o estado atual.

---

## 🧠 Solução
Implementação de uma **dimensão SCD Type 2**, onde cada mudança relevante gera uma **nova versão do registro**, preservando todo o histórico.

A solução contempla:

- Versionamento por **chave de negócio**
- **Fechamento automático** da versão anterior
- Inserção de **nova versão ativa**
- Detecção de mudança via **hash de atributos**
- **Idempotência** (reprocessamentos seguros)
- Persistência transacional com **Delta Lake**

---

## 🏗️ Arquitetura Lógica

### 📥 Fonte — Snapshot Operacional
`financas.ops_finance.ar_open_items`

Tabela que representa o **estado mais recente** das entidades no sistema operacional.

Principais campos:
- `account_group_id`
- `delinquency_risk_level`
- `ingested_at`

---

### 📤 Destino — Dimensão SCD Type 2
`financas.ops_finance.dim_ar_risk_scd2`


Tabela dimensional responsável por armazenar **todas as versões históricas**.

---

## 🧩 Modelo de Dados — Dimensão SCD2

| Coluna | Descrição |
|------|----------|
| `account_group_id` | Chave de negócio |
| `delinquency_risk_level` | Nível de risco versionado |
| `valid_from` | Início da vigência |
| `valid_to` | Fim da vigência |
| `is_current` | Flag de registro ativo |
| `scd_hash` | Hash dos atributos monitorados |
| `created_at` | Timestamp de criação |
| `updated_at` | Timestamp da última atualização |

---

## 🔄 Regras de Versionamento (SCD Type 2)

1. Para cada `account_group_id`, o pipeline seleciona o **registro mais recente** da fonte.
2. Um **hash** é calculado a partir dos atributos versionados.
3. O hash é comparado com o **registro ativo atual** na dimensão.
4. Quando há mudança:
   - o registro atual é **encerrado** (`valid_to = data_atual - 1`)
   - `is_current` passa para `false`
   - uma **nova versão** é inserida com:
     - `valid_from = data_atual`
     - `valid_to = 9999-12-31`
     - `is_current = true`
5. Quando não há mudança, nenhuma ação é executada.
6. O processo é **idempotente**: múltiplas execuções não geram duplicidade.

---

## ⚙️ Stack Tecnológica
- Apache Spark (PySpark)
- Delta Lake
- Databricks
- Window Functions
- Hashing para change detection
- MERGE transacional (ACID)

---

## 🚀 Benefícios da Abordagem
- Histórico completo e auditável
- Separação clara entre **estado atual** e **estado histórico**
- Pronto para consumo por BI e analytics
- Compatível com reprocessamentos e cargas incrementais
- Padrão enterprise amplamente utilizado

---

## O que é SCD?

**SCD** significa **Slowly Changing Dimension**  
(em português: **Dimensão de Mudança Lenta**).

É um **padrão de modelagem dimensional** usado em **Data Warehouses e Lakehouses** para lidar com **atributos que mudam ao longo do tempo**, mas **não mudam a todo instante**.

---

## 💡 Ideia Central
Alguns dados **mudam**, e simplesmente sobrescrevê-los faz com que o **histórico seja perdido**.

Exemplo:
- Hoje uma conta possui **risco = BAIXO**
- No futuro, esse risco muda para **ALTO**

Perguntas que o negócio costuma fazer:
- Qual era o risco dessa conta no passado?
- Quando ocorreu a mudança?
- Quanto tempo permaneceu em cada nível?

Sem SCD → perde-se o histórico  
Com SCD → o tempo passa a ser parte do dado

---

## 📦 Onde o SCD é aplicado
O SCD é utilizado em **tabelas de dimensão**, não em fatos.

Dimensões comuns:
- cliente
- conta
- produto
- fornecedor
- risco
- status

Essas entidades possuem **atributos descritivos** que podem mudar ao longo do tempo.

---

## 🧱 Tipos de SCD (principais)

### 🔹 SCD Tipo 0 — Não muda
- O valor nunca é alterado  
- Exemplo: data de nascimento

---

### 🔹 SCD Tipo 1 — Sobrescrita
- Atualiza o valor antigo  
- **Não preserva histórico**  
- Simples e barato

---

### 🔹 SCD Tipo 2 — Versionamento (mais importante)
- Cada mudança gera **uma nova linha**
- Mantém **histórico completo**
- Usa controle de vigência

Campos típicos:
- `valid_from`
- `valid_to`
- `is_current`

👉 É o padrão mais utilizado em ambientes analíticos corporativos.

---

### 🔹 SCD Tipo 3 — Histórico limitado
- Mantém apenas o valor atual e o anterior
- Uso restrito e pouco comum hoje

---

## 🚀 Por que SCD Tipo 2 é tão valorizado
Ele permite:
- auditoria de dados
- análises históricas corretas
- rastreabilidade temporal
- compliance e governança
- reconstrução do estado passado

Em resumo: **dados explicáveis ao longo do tempo**.

---

## Quando **NÃO** usar SCD Type 2

SCD Type 2 é poderoso, mas **não é bala de prata**.  
Usá-lo sem critério gera **custo, complexidade e dados inúteis**.

Abaixo estão os cenários em que **não faz sentido** usar SCD2 — visão prática, de engenharia sênior.

---

## ❌ 1. Quando o histórico **não tem valor analítico**
Se o negócio **nunca** vai perguntar “como era antes”, não versiona.

Exemplos:
- flag técnica (`is_active`)
- status operacional transitório
- campos calculados ou derivados

👉 Use **SCD1** (sobrescrita simples).

---

## ❌ 2. Quando o atributo muda o tempo todo
Alta volatilidade + SCD2 = explosão de linhas.

Exemplos ruins para SCD2:
- saldo financeiro
- quantidade em estoque
- métricas operacionais em tempo real

👉 Isso é **fato**, não dimensão.

---

## ❌ 3. Quando o dado é apenas um snapshot atual
Se a fonte já representa **o estado atual** e não há necessidade de reprocessar o passado, SCD2 só adiciona ruído.

Exemplo:
- tabelas “current_state”
- cadastros técnicos de controle interno

👉 Use tabela de **estado atual** ou **change tracking**.

---

## ❌ 4. Quando o histórico já está no fato
Se a granularidade temporal já existe na **tabela fato**, duplicar isso na dimensão é desperdício.

Exemplo:
- fato de pedidos com data de status
- eventos com timestamp próprio

👉 Dimensão simples + fato temporal resolve.

---

## ❌ 5. Quando o custo não compensa
SCD2 aumenta:
- armazenamento
- custo de processamento
- complexidade de queries
- tempo de carga

Se o volume for alto e o valor analítico baixo:
👉 **não use**.

---

## ❌ 6. Quando o dado é imutável
Alguns atributos **não deveriam mudar**.

Exemplos:
- CPF / CNPJ
- data de nascimento
- identificadores legais

👉 Modelagem errada se mudar → corrigir na origem, não versionar.

---

## ⚠️ Alternativas ao SCD2

| Cenário | Abordagem correta |
|------|------------------|
| Histórico irrelevante | SCD1 |
| Estado atual apenas | Snapshot |
| Mudança frequente | Fato |
| Apenas detectar mudança | Change Tracking |
| Histórico curto | SCD3 |

---

## 🧠 Regra de ouro
> **Use SCD2 apenas quando o tempo for parte da pergunta de negócio.**

Se ninguém pergunta “quando mudou” ou “como era antes”, **SCD2 é overengineering**.

---

## 📈 Possíveis Evoluções
- Inclusão de **surrogate key** por versão
- Versionamento de múltiplos atributos
- Granularidade por timestamp (`valid_from_ts`)
- Particionamento por data para ganho de performance
- Políticas de retenção histórica controladas

---

## 🧠 Conceitos Demonstrados
- Slowly Changing Dimension (Type 2)
- Change detection por hash
- Controle de vigência temporal
- Engenharia de dados orientada a governança
- Design para pipelines idempotentes

---

## 📬 Considerações Finais
Este repositório foi construído para demonstrar **maturidade técnica em engenharia de dados**, com foco em **modelagem dimensional**, **governança de dados** e **boas práticas em ambientes Databricks/Lakehouse**.

É um exemplo direto, reutilizável e alinhado ao que se espera em **ambientes corporativos de médio e grande porte**.
> Este projeto demonstra não apenas a implementação de SCD Type 2,  
> mas também o **discernimento arquitetural** sobre quando e por que utilizá-lo.
