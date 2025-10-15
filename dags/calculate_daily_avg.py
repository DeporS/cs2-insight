import logging

from db_init import calculate_daily_avg as calculate_daily_avg_db

def calculate_daily_avg_task():
    logging.info("Calculating daily average prices...")
    calculate_daily_avg_db() # Calculates daily averages and saves to daily_avg_skin_prices table
    logging.info("Daily average prices calculated and saved.")