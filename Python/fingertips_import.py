import requests
import pandas as pd
import io
import logging
from tqdm import tqdm
from collections import defaultdict
import asyncio
import aiohttp

# Set up logging
logging.basicConfig(
    filename='ftp_import_log.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_fingertips_indicators(indicator_ids, area_type_ids=None, area_codes=None):
    """
    Downloads indicator data from the Fingertips API for one or more indicator IDs.
    Batches all indicator IDs into a single call (no area type filter).
    Filters results to only include specified area types and area codes (if provided).
    Returns a combined pandas DataFrame of all successful downloads.
    """
    base_url = "https://fingertips.phe.org.uk/api/all_data/csv/by_indicator_id"
    all_dataframes = []

    # Ensure indicator_ids is a list and comma-separated string for batching
    if isinstance(indicator_ids, (int, str)):
        indicator_ids = [indicator_ids]
    indicator_id_str = ",".join(str(i) for i in indicator_ids)

    # If area_type_ids is provided, ensure it's a list
    if area_type_ids is not None and isinstance(area_type_ids, (int, str)):
        area_type_ids = [area_type_ids]

    # If area_codes is provided, ensure it's a list
    if area_codes is not None and isinstance(area_codes, str):
        area_codes = [area_codes]

    # Download once for all indicator IDs (no area type or area code filter)
    url = f"{base_url}?indicator_ids={indicator_id_str}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        text = response.text
        if not text.strip() or text.strip().startswith("No data"):
            logging.warning(f"No data returned for indicator IDs {indicator_id_str}")
            return pd.DataFrame()
        df = pd.read_csv(io.StringIO(text), dtype=str, low_memory=False)
        if df.empty:
            logging.warning(f"Empty DataFrame for indicator IDs {indicator_id_str}")
            return pd.DataFrame()
        all_dataframes.append(df)
        logging.info(f"Successfully retrieved indicators {indicator_id_str}")
    except Exception as e:
        logging.error(f"Error retrieving indicators {indicator_id_str}: {e}")
        return pd.DataFrame()

    # Combine all dataframes (only one in this case)
    combined_df = pd.concat(all_dataframes, ignore_index=True)

    # Filter by area_type_ids if provided
    if area_type_ids is not None:
        for col in ['AreaTypeId', 'area_type_id', 'AREATYPEID']:
            if col in combined_df.columns:
                combined_df = combined_df[combined_df[col].astype(str).isin([str(a) for a in area_type_ids])]
                break

    # Filter by area_codes if provided
    if area_codes is not None:
        for col in ['AreaCode', 'area_code', 'AREACODE']:
            if col in combined_df.columns:
                combined_df = combined_df[combined_df[col].isin(area_codes)]
                break
    return combined_df


# list area types
def list_area_types():
    """
    Fetches and returns a DataFrame of available area types from the Fingertips API.
    Prints errors to console if the request fails.
    """
    url = "https://fingertips.phe.org.uk/api/area_types"

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise error for bad status codes
        data = response.json()
        df = pd.DataFrame(data)
        print(f"✅ Retrieved {len(df)} area types.")
        return df
    except Exception as e:
        print(f"❌ Error retrieving area types: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure
