FROM astrocrpublic.azurecr.io/runtime:3.3-12

LABEL org.opencontainers.image.title="Task 3 Currency ETL" \
      org.opencontainers.image.description="Daily Frankfurter exchange rates to MongoDB with Airflow and named XComs" \
      org.opencontainers.image.authors="Hanzala" \
      org.opencontainers.image.version="1.0.0"
