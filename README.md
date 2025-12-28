# CENG3005 - Airline Management System (Big Data Implementation)

This project is a high-performance Relational Database Management System designed to handle and analyze over **23.8 million aviation records**. It utilizes a normalized 3NF schema and an RDF-style vertical metrics structure to optimize analytical queries.

## 👥 Team Members
* **Ali Emre Meşe** 
* **Özay Kaya** 
* **Görkem Can**

## 🚀 Key Features
* **Massive Data Handling:** Successfully migrated and managed a dataset of **1.05M flights** expanded into **23.8M performance metrics**.
* **3NF Normalized Schema:** Advanced geographical hierarchy (States -> Cities -> Airports) to ensure data integrity.
* **Python ETL Pipeline:** Custom automated scripts for batch data loading, state mapping, and referential integrity fixes.
* **Advanced DB Objects:** Optimized **Stored Procedures** (with IN/OUT parameters) and **Views** for real-time operational analysis.

## 🛠️ Tech Stack
* **Database:** MySQL Server 9.4.0
* **Management Tool:** MySQL Workbench 8.0.43
* **Programming:** Python 3.x (Pandas, MySQL-Connector)
* **Environment:** Windows 11

## 📂 Repository Structure
* `/ETL_Scripts`: Python files for data ingestion and cleaning.
* `/SQL_Scripts`: Schema creation (DDL), backend logic (Procedures/Views), and analytical queries.
* `/Documentation`: Project report and EER diagrams.

## 📊 Sample Analysis
The system is capable of calculating complex aggregates (like total state-level delays) across millions of rows in seconds using the implemented `GetStateTotalDelay` procedure.
