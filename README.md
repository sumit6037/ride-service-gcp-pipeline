# Data Engineering for Ride Service Provider Company

## Keywords
- Data Engineering
- Data Pipeline
- Data Transformation
- Business Analytics

## Abstract
This project is part of a customer engagement with a ride service provider based in multiple African countries. The customer offers services such as ride-hailing, deliveries, and white-labeling to various operators. This project focuses on the white-labeling aspect, capturing driver registrations and order flow data from the customer's major aggregator. The data collected is used to create visualizations that help the operations team make informed, time-conscious business decisions.

## Table of Contents
- [Introduction](#introduction)
- [Data Engineering](#data-engineering)
  - [Data Sources](#data-sources)
  - [Data Collection Strategy](#data-collection-strategy)
  - [Data Cleaning](#data-cleaning)
  - [Data Transformation](#data-transformation)
  - [Data Loading](#data-loading)
  - [Data Privacy](#data-privacy)
  - [Data Modeling](#data-modeling)
  - [Data Quality](#data-quality)
- [Data Visualization](#data-visualization)
- [Site Reliability Engineering](#site-reliability-engineering)
  - [Code Integration](#code-integration)
  - [Deployment Procedures](#deployment-procedures)
  - [Operations Team Training and Adoption](#operations-team-training-and-adoption)
  - [Monitoring and Observability](#monitoring-and-observability)
- [Conclusion](#conclusion)

## Introduction
The business is expanding across Africa, providing ride services, logistics, and white-labeling options. The white-labeling service allows other operators to use the customer's platform to book rides. This project aims to improve operational efficiency and customer satisfaction by consolidating and visualizing data from multiple API endpoints provided by vendors.

## Data Engineering

### Data Sources
The data is sourced from multiple API endpoints provided by the vendor travel operator, including real-time order updates via a websocket and various REST API endpoints for order and driver details.

### Data Collection Strategy
An Event-Driven Architecture is used for real-time data collection, leveraging Google Cloud services such as Compute Engine, Cloud Storage, Cloud Functions, and BigQuery.

### Data Cleaning
Duplicate data is removed, and checks are implemented to ensure data accuracy before loading it into BigQuery.

### Data Transformation
Data is transformed using SQL queries in Python scripts to generate normalized, structured datasets in BigQuery.

### Data Loading
Real-time data is loaded into BigQuery using Python scripts running on Compute Engine, with batch processes scheduled via crontab.

### Data Privacy
Data privacy measures include handling non-personal, static data, such as bank names and codes.

### Data Modeling
A data model is created based on the API responses, structuring data into various objects such as OrderRequest, Driver, Car, and Payment.

### Data Quality
Validation checks and data deduplication processes are implemented to ensure data quality.

## Data Visualization
Visualizations are created in Looker, connected to BigQuery, to help the operations team make data-driven decisions.

## Site Reliability Engineering

### Code Integration
GitHub is used for version control, with separate repositories for data processing and LookML code.

### Deployment Procedures
Code deployment is managed using GitHub Actions, with rollback mechanisms in place.

### Operations Team Training and Adoption
A comprehensive wiki is provided to the operations team, detailing the data models and LookML configurations.

### Monitoring and Observability
Google Cloud's Monitoring and Logging services are utilized for infrastructure and application monitoring.

## Conclusion
This project has empowered the business to manage operations more efficiently, enabling the operations team to make informed decisions and attract more vendors to their white-labeling portfolio.
