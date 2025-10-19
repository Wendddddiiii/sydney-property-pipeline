# Sydney Property Market Data Pipeline

## Project Summary
End-to-end data pipeline analyzing Sydney property market with 10k+ listings.

## Architecture
```
Data Source (Kaggle CSV)
    ↓
Data Loader (Python/Pandas)
    ↓
PostgreSQL Database (Raw Data)
    ↓
ETL Pipeline (Transformations)
    ↓
PostgreSQL (Processed Data)
    ↓
Airflow (Orchestration) ← Scheduled weekly
    ↓
Streamlit Dashboard (Visualization)
```

## Tech Stack
- **Data Processing**: Python, Pandas
- **Database**: PostgreSQL
- **Orchestration**: Apache Airflow
- **Visualization**: Streamlit, Plotly
- **Version Control**: Git, GitHub


## Future Improvements
- Add real-time data scraping from Domain API
- Implement dbt for transformation layer
- Deploy to AWS (RDS + EC2 + S3)
- Add ML price prediction model
- Set up CI/CD with GitHub Actions