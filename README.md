# Airflow ETL Pipeline

This project implements a fully automated ETL pipeline using Apache Airflow and Python, designed to streamline data extraction from multiple sources, transform it according to specific requirements and load it into a PostgreSQL database. The pipeline handles various file formats (.csv, .tsv, .txt) ensuring efficient data processing and seamless integration into a centralized database for further analysis.

# Tasks:  
  - Untar: The first task extracts the contents of a .tgz file containing the datasets.
  - Extracts data from various file formats (.csv, .tsv, .txt) and organizes the data into relevant structures.
  - Combines the extracted data and consolidates into a unified output format. 
  - Transforming process involves changing columnar data formats.
  - Loads the transformed dataset into a PostgreSQL database, ensuring seamless data integration and storage.

# Key features
  - Automation: Fully automated ETL pipeline that runs on a scheduled basis (daily).
  - Scalability: Easily adaptable for processing larger datasets by adjusting the schedule or task configuration.
  - Data Transformation: Data extraction, consolidation, and transformation into a unified format ready for storage.
  - PostgreSQL Integration: Loads processed data into PostgreSQL for easy access and future analytics.
  - 


<img src="02.png" width=850 height=400>
<img src="01.png" width=850 height=400>
<img src="04.png" width=850 height=400>
<img src="03.png" width=600 height=400>
