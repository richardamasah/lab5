import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger()

# Paths to raw xlsx files (simulated local drop zone)
orders_xlsx = "raw_xlsx/orders_apr_2025.xlsx"
order_items_xlsx = "raw_xlsx/order_items_apr_2025.xlsx"

# Output structure (mimicking S3 raw zone)
orders_output_dir = "lakehouse/raw/orders"
order_items_output_dir = "lakehouse/raw/order_items"
os.makedirs(orders_output_dir, exist_ok=True)
os.makedirs(order_items_output_dir, exist_ok=True)

def split_by_date_and_save(df, date_column, base_name, output_dir):
    try:
        unique_dates = df[date_column].dropna().unique()
        for date in unique_dates:
            daily_df = df[df[date_column] == date]
            output_file = f"{base_name}_{date}.csv"
            output_path = os.path.join(output_dir, output_file)
            daily_df.to_csv(output_path, index=False)
            logger.info(f"Saved: {output_path}")
    except Exception as e:
        logger.error(f"Error splitting {base_name} data: {e}")

def process_xlsx_to_csv(file_path, date_column, base_name, output_dir):
    try:
        logger.info(f"Reading {file_path}")
        df = pd.read_excel(file_path)
        split_by_date_and_save(df, date_column, base_name, output_dir)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")

# Process orders and order_items
process_xlsx_to_csv(orders_xlsx, "date", "orders", orders_output_dir)
process_xlsx_to_csv(order_items_xlsx, "date", "order_items", order_items_output_dir)
