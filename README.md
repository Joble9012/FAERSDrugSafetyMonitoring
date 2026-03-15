# Drug Safety Monitoring Pipeline (FAERS)

## Overview
This project builds a data pipeline in Databricks to analyze adverse drug events using the FDA Adverse Event Reporting System (FAERS) dataset from **2021–2023**. The goal is to monitor drug safety by identifying patterns in reported adverse events.

The pipeline follows a **Bronze → Silver → Gold** architecture:
- **Bronze:** Raw FAERS data
- **Silver:** Cleaned and validated datasets
- **Gold:** Aggregated tables used for analysis and dashboards

## Business Question
A pharmaceutical company has released several drugs in recent years, and the Head of Drug Safety wants to closely monitor their safety performance. The main concern is determining whether any of these drugs are showing unexpected increases in reported adverse events compared to previous years.

This project analyzes FAERS data from **2021–2023** to identify patterns in adverse event reports. The analysis focuses on trends in reported adverse events, severe patient outcomes, and differences across patient demographics such as age groups and gender.

## Data Sources
FAERS dataset tables used:
- **Demographics** – Patient information such as age group and gender
- **Drug** – Reported drugs involved in adverse events
- **Reaction** – Reported adverse reactions
- **Outcome** – Reported patient outcomes

## Pipeline Steps
1. Load FAERS datasets (2021–2023) into Databricks.
2. Validate incoming data using defined **data contracts**.
3. Create **Silver tables** with cleaned and standardized data.
4. Generate **Gold tables** to answer key drug safety questions.
5. Visualize the results in a **Databricks dashboard**.

## Key Analyses
The Gold layer produces tables used to answer the following questions:

- Which drugs have the most adverse event reports over time?
- What are the most common reactions reported for each drug?
- Which drugs are associated with severe outcomes?
- Which age groups report the most adverse events?
- How do adverse events differ between male and female patients?