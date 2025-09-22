# Airflow EDA

> This is a sandbox project to improve my skills in Data Science.

## Project Dataset

Auto Sales dataset from [Kaggle](https://www.kaggle.com/datasets/ddosad/auto-sales-data)

## Project Background

An automotive company (or automotive retail distributor) wants to understand the sales patterns of its products across various countries, product segmentation, and customer profiles. The “Auto Sales” dataset contains customer order transactions, including quantities, prices, total sales values, customer countries, and shipment status.

## EDA Objectives

1. Understand the characteristics of the sales data (total number of orders, sales range, and top-selling product lines).
2. Analyze customer behavior based on country, city, and deal size.
3. Identify sales trends over time (ORDERDATE) and DAYS_SINCE_LASTORDER.
4. Detect product line patterns (Classic Cars, Motorcycles, etc.) to see which ones perform best.
5. Evaluate order status (Shipped, Cancelled, Disputed) to assess service quality.
6. Generate insights useful for marketing and inventory strategies.

## Tech Stack

- Python
- Pandas
- Airflow
- PostgreSQL
- ElasticSearch
- Docker

## Usage

- To run this project

  ```shell
  docker compose up -d
  ```
