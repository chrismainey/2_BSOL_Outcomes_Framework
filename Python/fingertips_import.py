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

def get_fingertips_indicators(indicator_ids, area_type_ids=None, area_codes=None, max_concurrent=8):
    """
    Downloads indicator data from the Fingertips API for one or more indicator IDs.
    Batches all indicator IDs into a single call per area type/area code.
    Uses asynchronous requests for speed.
    Returns a combined pandas DataFrame of all successful downloads.
    """
    base_url = "https://fingertips.phe.org.uk/api/all_data/csv/by_indicator_id"
    all_dataframes = []

    # Track summary info
    success_log = []
    missing_log = defaultdict(list)
    error_log = defaultdict(list)

    # Ensure indicator_ids is a list and comma-separated string for batching
    if isinstance(indicator_ids, (int, str)):
        indicator_ids = [indicator_ids]
    indicator_id_str = ",".join(str(i) for i in indicator_ids)

    # If no area types provided, fetch all available
    if area_type_ids is None:
        try:
            area_type_response = requests.get("https://fingertips.phe.org.uk/api/area_types")
            area_type_response.raise_for_status()
            area_type_data = area_type_response.json()
            area_type_ids = [area['Id'] for area in area_type_data]
            logging.info(f"Retrieved {len(area_type_ids)} area types.")
        except Exception as e:
            logging.error(f"Failed to retrieve area types: {e}")
            return pd.DataFrame()

    # If area_codes is provided, ensure it's a list
    if area_codes is not None and isinstance(area_codes, str):
        area_codes = [area_codes]

    # Prepare all combinations (batching indicator IDs)
    tasks = []
    if area_codes:
        for area_type_id in area_type_ids:
            for area_code in area_codes:
                tasks.append((indicator_id_str, area_type_id, area_code))
    else:
        for area_type_id in area_type_ids:
            tasks.append((indicator_id_str, area_type_id, None))

    async def fetch_data(session, sem, task):
        indicator_id_str, area_type_id, area_code = task
        if area_code:
            url = f"{base_url}?indicator_ids={indicator_id_str}&area_type_id={area_type_id}&area_code={area_code}"
        else:
            url = f"{base_url}?indicator_ids={indicator_id_str}&area_type_id={area_type_id}"
        async with sem:
            try:
                async with session.get(url) as response:
                    if response.status != 200:
                        raise Exception(f"HTTP {response.status}")
                    text = await response.text()
                    if not text.strip() or text.strip().startswith("No data"):
                        if area_code:
                            msg = f"{indicator_id_str} not available at area type {area_type_id} for area code {area_code}"
                            missing_log[indicator_id_str].append((area_type_id, area_code))
                        else:
                            msg = f"{indicator_id_str} is not available at area type {area_type_id}"
                            missing_log[indicator_id_str].append(area_type_id)
                        logging.warning(msg)
                        return None
                    df = pd.read_csv(io.StringIO(text), dtype=str, low_memory=False)
                    if df.empty:
                        if area_code:
                            msg = f"{indicator_id_str} not available at area type {area_type_id} for area code {area_code}"
                            missing_log[indicator_id_str].append((area_type_id, area_code))
                        else:
                            msg = f"{indicator_id_str} is not available at area type {area_type_id}"
                            missing_log[indicator_id_str].append(area_type_id)
                        logging.warning(msg)
                        return None
                    if area_code:
                        msg = f"Successfully retrieved indicators {indicator_id_str} for area type {area_type_id} and area code {area_code}"
                        success_log.append((indicator_id_str, area_type_id, area_code))
                    else:
                        msg = f"Successfully retrieved indicators {indicator_id_str} for area type {area_type_id}"
                        success_log.append((indicator_id_str, area_type_id))
                    logging.info(msg)
                    return df
            except Exception as e:
                if area_code:
                    msg = f"Error retrieving indicators {indicator_id_str} for area type {area_type_id} and area code {area_code}: {e}"
                    error_log[indicator_id_str].append((area_type_id, area_code, str(e)))
                else:
                    msg = f"Error retrieving indicators {indicator_id_str} for area type {area_type_id}: {e}"
                    error_log[indicator_id_str].append((area_type_id, str(e)))
                logging.error(msg)
                return None

    async def main():
        sem = asyncio.Semaphore(max_concurrent)
        async with aiohttp.ClientSession() as session:
            coros = [fetch_data(session, sem, task) for task in tasks]
            for f in tqdm(asyncio.as_completed(coros), total=len(coros), desc="Fetching data"):
                df = await f
                if df is not None:
                    all_dataframes.append(df)

    asyncio.run(main())

    # Print log summary
    print("\n📋 Log Summary:")
    print(f"✅ Successful downloads: {len(success_log)}")
    print(f"⚠️ Missing indicator-area combinations: {sum(len(v) for v in missing_log.values())}")
    print(f"❌ Errors encountered: {sum(len(v) for v in error_log.values())}")

    if missing_log:
        print("\nMissing combinations:")
        for ind, areas in missing_log.items():
            print(f"  Indicator(s) {ind} missing at area types/codes: {areas}")

    if error_log:
        print("\nErrors:")
        for ind, errors in error_log.items():
            for err in errors:
                print(f"  Indicator(s) {ind} at {err}")

    # Return combined DataFrame
    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        return combined_df
    else:
        logging.warning("No data retrieved.")
        return pd.DataFrame()


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
