# Airflow-ETL
Python ETL Pipeline to PostgresDB

This project implements a fully automated ETL pipeline using Apache Airflow and Python, designed to streamline data extraction from multiple sources, transform it according to specific requirements, and load it into a PostgreSQL database. The pipeline handles various file formats (.csv, .tsv, .txt), ensuring efficient data processing and seamless integration into a centralized database for further analysis.

# Features:  
  - Untar: The first task extracts the contents of a .tgz file containing the datasets.
  - Extracts data from various file formats (.csv, .tsv, .txt) and organizes the data into relevant structures.
  - Combines and transforms the extracted data, such as consolidating files and changing columnar data formats, before storing it in a unified output format.
  - Loads the transformed dataset into a PostgreSQL database, ensuring seamless data integration and storage.
  
# Workflow:
  - The pipeline is designed as an Apache Airflow Directed Acyclic Graph (DAG), automating the ETL tasks on a scheduled basis (daily).

<img src="screenshots/Screenshot 2025-03-21 at 16.33.22.png" width=500 height=400>
