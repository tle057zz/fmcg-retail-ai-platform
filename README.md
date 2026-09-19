# FMCG Retail Data & AI Platform

Project folder structure (empty scaffolding — build step by step).

```text
fmcg-retail-ai-platform/
├── data/raw/              # dunnhumby CSVs (next step)
├── src/                   # ingestion, validation, transforms, features, utils
├── notebooks/             # Databricks notebooks 01–10
├── dbt/                   # staging → intermediate → marts
├── sql/                   # quality checks + analytics
├── ml/                    # training, evaluation, inference
├── ai/                    # prompts, RAG, agent, evaluation
├── powerbi/documentation/
├── tests/
└── docs/
```

## Next step

Dataset is ready under `data/raw/`. Next: data exploration notebook (`01_data_exploration`).
