# 📊 Volume Monitoring with Business Rules (Rule 7 and Rule 8)

This project implements a data monitoring pipeline to identify significant changes in vehicle financing volumes across two reference periods. The goal is to apply business rules to assess data consistency, detect outliers, and analyze records segmented by document type (Individual or Corporate) and vehicle category (Light, Motorcycle, Heavy).

---

## 🧠 Objective

- Calculate the **overall volume** of records in two different processing dates.
- Compare data volumes **with and without outlier removal**.
- Classify results based on **percentage variation**.
- Generate **indicators and status labels** to support decision-making.

---

## ⚙️ Technologies Used

- **PySpark** (DataFrame API)
- **AWS Glue**
- **Amazon S3**
- **Athena**
- **SQL**
- **Jupyter Notebook** (for local testing)

---

## 🛠️ Implemented Rules

### ✅ Rule 7 — Overall Volume

Evaluates data volume by document type and vehicle category **without outlier filtering**.

### ✅ Rule 8 — Overall Volume with Outlier Removal

Applies multiple filters to exclude outliers before comparing data volumes. Filters include:

- `vlr_tot_financ >= 1000`
- `vlr_tot_financ <> 999999.99 AND <> 9999999.99`
- `idade_compra >= -1`
- Specific flags: `flag_tx_juros = 'TRUE'`, `flag_tx_usado = 'FALSE'`, etc.
- Non-null constraints: `tipo_pessoa`, `ct_veic_tipo`, `ct_veic_idade`, `uf_licenciamento`, `flag_cvg`, `flag_consorcio`, `cnpj_credor`

---

## 📊 Metrics Calculated

- **current_metric_value**: Current volume count.
- **previous_metric value**: Previous volume count.
- **percentage_variation**: Percentage change.
- **nome_status_metrica**: Status label (OK, ATTENTION, NOT OK).
- **descricao_status_metrica**: Textual explanation of the percentage range.

---

## ✅ Execution Example

```python
regra8_cubo(sqlContext, "owner", "monitoring", "20241130", "20241031")
```

---

## 📅 Project Duration

The project was developed over the course of **3 month**, including:

- Understanding complex **business rules**
- Developing the pipeline in **PySpark**
- Implementing **outlier detection and filtering**
- Performing tests and validating results across different data sources

---

## 🚀 Results

The metrics generated were used to feed **analytical dashboards**, support data auditing, and ensure the quality and consistency of operational information related to vehicle financing.

---

## 🙌 Contributions

This project was developed individually as part of a technical study in **data engineering and performance metric monitoring**. Suggestions and improvements are welcome!


## 📌 Author

Developed by a Data Engineer specializing in PySpark, Data Quality, and Volume Monitoring for large-scale datasets.
