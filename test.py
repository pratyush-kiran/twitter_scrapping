import pandas as pd
import pytz
from datetime import datetime, timedelta

# def capture_last_fetch_datetime():
#     last_fetch_datetime_data = [{
#         "Last Fetch DateTime" : File_Created_Date
#     }]

#     last_fetch_datetime_df = pd.DataFrame(last_fetch_datetime_data)
#     last_fetch_datetime_df.to_csv('last_fetch_datetime.csv', index=False)
#     print("Last fetch date captured :", File_Created_Date)



def get_start_time():
    df = pd.read_csv("last_fetch_datetime.csv")
    start_time = df['Last Fetch DateTime'][0]

    ist_timezone = pytz.timezone("Asia/Kolkata")

    # Convert string to datetime object in IST
    ist_datetime = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    ist_datetime = ist_timezone.localize(ist_datetime)

    # Convert to UTC
    utc_datetime = ist_datetime.astimezone(pytz.utc)

    # Format as "%Y-%m-%dT%H:%M:%S.%fZ"
    formatted_utc_str = utc_datetime.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    
    return formatted_utc_str

print(get_start_time())