# Student Vault: Institutional Data Infrastructure & ETL Pipeline

## Executive Summary
Developed **Student Vault**, an end-to-end data engineering solution that automates the extraction and transformation of legacy student portal data. By implementing a **three-tier Medallion-style architecture** and **SCD Type 2 modeling**, I achieved a **125x data compression ratio**, transforming 250,000+ raw records into a streamlined 2,000-row analytical layer.

---

## The Problem: Underleveraged Data Assets
The University Student Portal contained years of academic records, yet it remained an underleveraged resource. Because data was trapped and underleveraged, high-value administrative tasks—like the weeks-long manual verification of President's and Dean's List honors—required hundreds of man-hours of manual GWA calculation. The university had the data, but lacked the **analytical infrastructure** to use it.

---

## Technical Solution

### 1. Ingestion Engine & Prototype UI
* **UI Prototype:** Developed a custom front-end (**Student Vault**) to showcase potential UX improvements and analytical functionalities.
* **Extraction:** Built a Python-based ingestion engine to scrape the existing University Portal’s Grade Endpoint, handling complex session authentication and HTML parsing to migrate data from the legacy system into the Vault ecosystem.

### 2. The 3-Layer Analytical Warehouse
To ensure efficiency and scalability, I implemented a Medallion-style architecture:

| Layer | Type | Description |
| :--- | :--- | :--- |
| **Bronze** | **Raw** | Preserved granular grade data (Initial pilot: ≈250k rows for 500 students) for full auditability. |
| **Silver** | **Semestral** | **Operational Truth:** Programmatically aggregated individual grades into semestral GWAs. This serves as the **Single Source of Truth** for immediate actions, such as honors assessment. |
| **Gold** | **Yearly** | **Historical Truth:** Applied **SCD Type 2 Modeling** to track academic status evolution and maintain historical snapshots of the student journey. |

### 3. Proof of Concept: Automated Honors Assessor
Built the first functional module atop the **Silver Layer** to instantly identify Dean’s and President’s List candidates, a task that previously required manual cross-referencing of hundreds of physical and digital records.

---

## Compliance & Ethics
Navigated the portal’s "no-scraping" Terms of Service by pitching the project as a **systemic solution** rather than a simple script. This professional transparency successfully secured formal approval from the **College Dean** to move toward full institutional integration.

---

## The Result
The project successfully transitioned from a personal technical challenge to an institutional pilot program. By demonstrating the scalability of the 3-layer architecture, the project is currently undergoing full integration, with the potential to automate workflows for over **42,000 students**.
