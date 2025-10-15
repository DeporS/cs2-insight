import logging

from db_init import calculate_stats_and_alerts

def calculate_stats_task():
    logging.info("Calculating stats and alerts...")
    calculate_stats_and_alerts() # Calculates stats and saves to skin_price_stats table
    logging.info("Stats and alerts calculated and saved.")