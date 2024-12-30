# ShopMart-Data-Pipeline

## Introduction
This is a data pipeline for extracting, transforming, and loading data from the [Fakestore API](https://fakestoreapi.com/docs). It processes product, user, and cart data into tables for analytics. This pipeline focuses more on Snowflake for transformation and loading.

## Data Model
![Blank diagram - ecom datamodel](https://github.com/user-attachments/assets/c473daa3-61b4-4475-9692-2282a2747f59)

## Data Pipeline Architecture
![Blank diagram - Page 3 (2)](https://github.com/user-attachments/assets/a77ebff4-2378-4870-a4b0-0fdeefa587e6)

### Architecture Overview
The pipeline leverages the following components:
- **AWS Lambda**: Extracts raw data from the FakeStore API and stores it in S3.
- **AWS S3**: Acts as the raw data repository.
- **Snowflake Snowpipe**: Automatically ingests JSON data into Snowflake stage tables.
- **Snowpark**: Transforms the data into structured, analytics-ready tables.
- **Snowflake Streams, Procedures, and Tasks**: Automates ongoing data updates and transformations.

## Tech Stack / Data Flow
- **AWS Lambda**
  - Fetches API data and organizes it in S3:
      - user_raw/
      - product_raw/
      - cart_raw/
- **AWS S3**
    - Stores JSON data for users, products, and carts in separate folders.

- **Snowflake Snowpipe**
  Loads the raw JSON files into Snowflake stage tables:
    - USER_STAGE
    - PRODUCT_STAGE
    - CART_STAGE

- **Snowpark**
    - Transforms raw stage data into analytics-ready tables:
      - DIM_USER and DIM_ADDRESS from user data.
      - DIM_PRODUCT from product data.
      - FACT_CART from cart data.

- **Snowflake Streams, Procedures, and Tasks**
  - Automates the ETL process:
  - Streams: Tracks new data in stage tables.
  - Procedures: Handles transformation logic.
  - Tasks: Schedules regular updates.


## Key Features of the Pipeline
- Simplified Workflows: Direct transformations in Snowflake.
- Scalability: Handles large datasets effortlessly.
- Python Integration: Developer-friendly scripting.
- Performance: Optimized for Snowflake.
