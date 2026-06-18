import os
import duckdb
import requests
from dotenv import load_dotenv

#1. Load .env file
load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")

if not API_KEY:
    raise ValueError("Can't find OPENWEATHER_API_KEY，please check .env file！")

#2. Define the cities we want to fetch weather data for
CITIES = ["Taipei,tw", "Xizhi,tw", "Banqiao,tw", "Tokyo,jp", "London,gb"]

def fetch_real_weather():
    weather_data_list = []
    
    print("Fetch weather data from OpenWeather API...")

    for city in CITIES:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}"
        
        print(f"Try to connect to: {url}")

        try:
            response = requests.get(url, timeout=5)

            if response.status_code == 200:
                data = response.json()
                
                weather_entry = {
                    "city": data["name"],
                    "weather": data["weather"][0]["main"],
                    "temp_k": data["main"]["temp"],      # Raw data is (Kelvin)
                    "dt": data["dt"]                    
                }
                weather_data_list = weather_data_list + [weather_entry]
                print(f"✅ Successfully fetched {data['name']}: {weather_entry['weather']}, {weather_entry['temp_k']}K")
            else:
                print(f"❌ Failed to fetch weather data for city {city}, Status Code: {response.status_code}")

        except requests.exceptions.Timeout:
                print(f"【連線逾時】呼叫 {city} 時網路超時 5 秒，OpenWeather 伺服器沒回應！")
        except requests.exceptions.RequestException as e:
                print(f"【網路異常】發生未知錯誤: {e}")

    return weather_data_list

def save_to_duckdb(data):
    if not data:
        print("⚠️ No data fetched, canceling database write.")
        return
    
    conn = duckdb.connect("local_weather.duckdb")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS main.raw_weather_data (
            city VARCHAR,
            weather VARCHAR,
            temp_k DOUBLE,
            dt BIGINT
        )
    """)

    # Truncate old raw table before inserting new data
    conn.execute("TRUNCATE main.raw_weather_data")

    # batch insert new data
    for row in data:
        conn.execute("""
            INSERT INTO main.raw_weather_data VALUES (?, ?, ?, ?)
        """, (row["city"], row["weather"], row["temp_k"], row["dt"]))
        
    conn.close()
    print("💾 Successfully inserted latest data into raw_weather_data table!")

if __name__ == "__main__":
    raw_data = fetch_real_weather()
    save_to_duckdb(raw_data)

