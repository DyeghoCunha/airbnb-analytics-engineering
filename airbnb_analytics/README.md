# 🏠 Airbnb Analytics Engineering Project

[![dbt CI/CD](https://github.com/seu-usuario/airbnb-analytics-engineering/actions/workflows/dbt_ci.yml/badge.svg)](https://github.com/seu-usuario/airbnb-analytics-engineering/actions/workflows/dbt_ci.yml)
[![dbt Version](https://img.shields.io/badge/dbt-1.7.0-orange)](https://www.getdbt.com/)
[![Databricks](https://img.shields.io/badge/Databricks-Unity%20Catalog-red)](https://databricks.com/)

## 📊 Project Overview

End-to-end analytics engineering project implementing a **Medallion Architecture** (Bronze → Silver → Gold) for Airbnb listing and review data using modern data stack:

- **dbt Core** for transformation & modeling
- **Databricks** (Unity Catalog) as data lakehouse
- **GitHub Actions** for CI/CD automation
- **Power BI** for business intelligence

---

## 🏗️ Architecture
```
┌─────────────────┐
│  Bronze Layer   │ ← Raw data ingestion
│  (Source)       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Silver Layer   │ ← Data cleaning & standardization
│  (Staging)      │    - Deduplication
└────────┬────────┘    - Type casting
         │             - Feature engineering
         ▼
┌─────────────────┐
│   Gold Layer    │ ← Dimensional modeling (Star Schema)
│  (Marts)        │    - Fact tables
└─────────────────┘    - Dimension tables
         │
         ▼
┌─────────────────┐
│    Power BI     │ ← Business Intelligence & Reporting
└─────────────────┘
```

---

## 📁 Project Structure
```
airbnb-analytics-engineering/
├── .github/workflows/
│   └── dbt_ci.yml           # CI/CD pipeline
├── dbt_project/
│   ├── models/
│   │   ├── bronze/          # Source definitions
│   │   ├── silver/          # Staging models
│   │   └── gold/            # Dimensional models
│   ├── macros/              # Custom macros
│   ├── tests/               # Data quality tests
│   └── dbt_project.yml
├── docs/
├── powerbi/
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

- Python 3.10+
- Databricks workspace with SQL Warehouse
- GitHub account
- Power BI Desktop

### Installation

1. **Clone repository**
```bash
git clone https://github.com/seu-usuario/airbnb-analytics-engineering.git
cd airbnb-analytics-engineering
```

2. **Install dbt**
```bash
pip install dbt-databricks==1.7.0
```

3. **Configure profiles.yml**
```bash
# Create ~/.dbt/profiles.yml with your Databricks credentials
# See docs/setup.md for details
```

4. **Install dbt packages**
```bash
cd dbt_project
dbt deps
```

5. **Test connection**
```bash
dbt debug
```

---

## 🔧 Commands

### Development
```bash
# Compile models
dbt compile

# Run specific layer
dbt run --select silver
dbt run --select gold

# Run with tests
dbt build --select silver

# Run tests only
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### Production
```bash
# Full refresh
dbt run --full-refresh

# Incremental run (only new data)
dbt run --select gold,tag:fact
```

---

## 🧪 Data Quality

### Test Coverage

- **Generic tests**: uniqueness, not_null, relationships, accepted_values
- **Custom tests**: referential_integrity, accepted_range, no_nulls_in_columns
- **Singular tests**: business logic validation

### Run tests
```bash
# All tests
dbt test

# Specific model
dbt test --select stg_listings

# Specific layer
dbt test --select silver
```

---

## 📊 Data Models

### Silver Layer (Staging)

- `stg_hosts` - Cleaned host data
- `stg_listings` - Cleaned listing data with price normalization
- `stg_reviews` - Review data with sentiment analysis

### Gold Layer (Star Schema)

**Dimensions:**
- `dim_host` - Host attributes with aggregated metrics
- `dim_listing` - Listing attributes with quality scores
- `dim_date` - Calendar dimension
- `dim_location` - Location hierarchy

**Facts:**
- `fact_reviews` - Review transactions (grain: 1 row per review)
- `fact_listings_daily_snapshot` - Daily listing snapshots for trend analysis

---

## 🔄 CI/CD Pipeline

GitHub Actions automatically:

1. **Lint** - SQL style check with SQLFluff
2. **Compile** - Verify model syntax
3. **Test** - Run data quality tests
4. **Build** - Deploy to dev/prod based on branch
5. **Document** - Generate and publish dbt docs

**Branches:**
- `main` → Production deployment
- `develop` → Development environment
- `feature/*` → Compile & test only

---

## 📈 Key Metrics & KPIs

- **Host Performance**: total_listings, positive_review_rate, host_experience_level
- **Listing Quality**: listing_quality_score, popularity_tier, activity_status
- **Review Analytics**: sentiment distribution, review volume trends, keyword mentions

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|-----------|
| Data Platform | Databricks (Unity Catalog) |
| Transformation | dbt Core 1.7.0 |
| Orchestration | Databricks Workflows |
| Version Control | Git + GitHub |
| CI/CD | GitHub Actions |
| BI Tool | Power BI |
| Testing | dbt tests + custom SQL |

---

## 📚 Documentation

- [Setup Guide](docs/setup.md)
- [SQL Patterns](docs/sql_patterns.md)
- [dbt Commands Cheatsheet](docs/dbt_commands.md)
- [Data Dictionary](https://your-github-pages-url.github.io)

---

## 👤 Author

**Dyegho Cunha**
- LinkedIn: [linkedin.com/in/dyeghocunha](https://linkedin.com/in/dyeghocunha)
- Email: contato@dyeghocunha.com

---

## 📝 License

This project is for educational and portfolio purposes.
```

---

## ✅ Checklist FASE 4

### GitHub Actions
- [ ] Criar `.github/workflows/dbt_ci.yml`
- [ ] Adicionar secrets no GitHub:
  - `DATABRICKS_HOST`
  - `DATABRICKS_HTTP_PATH`
  - `DATABRICKS_TOKEN`
- [ ] Fazer primeiro commit e push
- [ ] Verificar pipeline rodando em Actions

### Testes de Qualidade
- [ ] Criar `tests/generic/test_no_nulls_in_columns.sql`
- [ ] Criar `tests/generic/test_referential_integrity.sql`
- [ ] Criar `tests/generic/test_accepted_range.sql`
- [ ] Criar `tests/data_quality/test_price_distribution.sql`
- [ ] Criar `tests/data_quality/test_review_date_logic.sql`
- [ ] Criar `tests/data_quality/test_host_listing_consistency.sql`
- [ ] Atualizar `schema.yml` com testes avançados

### Macros e Helpers
- [ ] Criar `macros/test_helpers.sql`
- [ ] Executar `dbt test` para validar tudo

### Documentação
- [ ] Atualizar `README.md`
- [ ] Executar `dbt docs generate`
- [ ] Verificar documentação em `localhost:8080`

---

## 🎯 Como configurar GitHub Secrets

1. Vá no seu repositório GitHub
2. `Settings` → `Secrets and variables` → `Actions`
3. Click `New repository secret`
4. Adicione os 3 secrets:
```
Name: DATABRICKS_HOST
Value: adb-xxxxx.azuredatabricks.net

Name: DATABRICKS_HTTP_PATH
Value: /sql/1.0/warehouses/xxxxx

Name: DATABRICKS_TOKEN
Value: dapi................................