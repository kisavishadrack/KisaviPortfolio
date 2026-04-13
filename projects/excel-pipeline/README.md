# 🧹 Excel Data Cleaning Pipeline
### Portfolio Project — Kisavi Shadrack | Data Analyst

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue)](https://python.org)
[![pandas](https://img.shields.io/badge/pandas-2.x-green)](https://pandas.pydata.org)
[![openpyxl](https://img.shields.io/badge/openpyxl-3.x-orange)](https://openpyxl.readthedocs.io)

---

## 📌 Project Overview

A **production-grade, class-based Python pipeline** that ingests a messy real-world Excel file,
detects and resolves over 15 categories of data quality issues, and produces four professional outputs:
a clean dataset, a summary report, a data validation report, and visualisations.

This project demonstrates skills directly relevant to roles in data engineering, data analysis,
and business intelligence — including ETL design, data validation, automated reporting, and Python
software architecture.

---

## 🗂️ Project Structure

```
data_cleaning_pipeline/
├── pipeline.py               # Main pipeline (5 classes, ~500 lines)
├── generate_messy_data.py    # Reproducible messy dataset generator
├── config/
│   └── config.yaml           # All settings — no hardcoded values
├── data/
│   └── messy_sales_data.xlsx # Input: 315 rows, 10 columns, 15+ issue types
├── output/
│   ├── clean_sales_data.xlsx # Output: clean, formatted Excel file
│   └── charts/               # 5 PNG visualisations
│       ├── 01_revenue_by_region.png
│       ├── 02_revenue_by_product.png
│       ├── 03_monthly_trend.png
│       ├── 04_status_distribution.png
│       └── 05_price_distribution.png
├── reports/
│   ├── summary_report.xlsx   # Multi-sheet business summary
│   └── validation_report.xlsx # All 317 issues catalogued by type
├── logs/
│   └── pipeline.log          # Timestamped run log
└── README.md
```

---

## ⚙️ Pipeline Architecture

The pipeline is built using **5 specialised classes**, each with a single responsibility:

| Class | Responsibility |
|---|---|
| `DataLoader` | Reads Excel, strips title rows & footers, normalises null markers |
| `DataValidator` | Scans raw data for issues — records but does NOT modify |
| `DataCleaner` | Applies all 10 cleaning transformations |
| `ReportGenerator` | Writes 3 formatted Excel outputs |
| `ChartGenerator` | Produces 5 publication-quality PNG charts |

A top-level `Pipeline` orchestrator wires them together, drives logging, and tracks stats.

---

## 🔍 Data Quality Issues Handled

| Category | Examples |
|---|---|
| **Duplicates** | 15 exact row duplicates removed |
| **Null values** | 12 null marker variants (`N/A`, `TBD`, `""`, `None`, etc.) |
| **Date parsing** | 8 invalid formats (`13/25/2023`, `Jan 2023`, `32-01-2024`) |
| **Inconsistent casing** | `NAIROBI`, `nairobi`, `Nairobi` → `Nairobi` |
| **Whitespace** | Double spaces in names standardised |
| **Mixed types** | Numeric columns with `"five"`, `"free"`, `""` |
| **Invalid ranges** | Negative quantities, zero prices, extreme outliers |
| **Revenue mismatches** | 14 rows where Revenue ≠ Quantity × Price |
| **Malformed IDs** | `ORD0023` rescued to `ORD-0023` |
| **Invalid emails** | `missing@`, `@nodomain.com`, `not-an-email` nullified |
| **Outlier detection** | Z-score flagging (threshold = 3.0 SD) |
| **Excel noise** | Title row, blank rows, footer notes stripped automatically |

---

## 📊 Outputs

### 1. Clean Excel File (`output/clean_sales_data.xlsx`)
Formatted, alternating-row-coloured dataset with all issues resolved.

### 2. Summary Report (`reports/summary_report.xlsx`) — 5 sheets
- Pipeline Run Summary
- Revenue by Region
- Revenue by Product
- Sales Rep Performance
- Monthly Revenue Trend

### 3. Validation Report (`reports/validation_report.xlsx`) — 2 sheets
- All 317 individual issues with row index, column, type, original value
- Issue summary by type with percentages

### 4. Charts (`output/charts/`)
- Revenue by Region (horizontal bar)
- Revenue by Product (bar)
- Monthly Revenue Trend (line + fill)
- Order Status Distribution (pie)
- Unit Price Distribution (histogram with median line)

---

## 🚀 How to Run

```bash
# Install dependencies
pip install pandas openpyxl matplotlib seaborn pyyaml tabulate

# Generate the messy input dataset
python generate_messy_data.py

# Run the full pipeline
python pipeline.py

# Use a custom config
python pipeline.py --config config/my_config.yaml
```

---

## 🔧 Configuration

All settings live in `config/config.yaml` — zero hardcoded values in the pipeline:

```yaml
cleaning:
  null_markers: ["N/A", "TBD", "", "Unknown", ...]
  valid_regions: ["Nairobi", "Mombasa", "Kisumu", "Eldoret"]
  outlier_zscore_threshold: 3.0
  revenue_tolerance_pct: 0.01
```

To adapt this pipeline to a new dataset, only the config needs to change.

---

## 📈 Pipeline Results (Sample Run)

```
Raw Rows Loaded         : 315
Clean Rows Output       : 300
Duplicates Removed      : 15
Unparseable Dates Fixed : 14
Revenue Mismatches Fixed: 14
Outliers Flagged        : 1
Validation Issues Found : 317
Run Time                : < 3 seconds
```

---

## 🛠️ Tech Stack

- **Python 3.10+**
- **pandas** — data manipulation
- **openpyxl** — Excel reading/writing with formatting
- **matplotlib / seaborn** — visualisations
- **PyYAML** — config management
- **logging** — structured run logs
- **dataclasses** — typed data structures
- **argparse** — CLI interface

---

## 👤 Author

**Kisavi Shadrack** — Data Analyst  
📧 shadrackkisavi4@gmail.com  
📍 Nairobi, Kenya  
🔗 [Portfolio](https://kisavishadrack.github.io/Shadrackanalyst.github.io/)
