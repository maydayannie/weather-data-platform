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
CITIES = ["Taipei,tw", "Xizhi,tw", "Banqiao,tw", "Kaohsiung,tw", "Bangkok,th", "Pattaya,th", "Tokyo,jp", "London,gb"]

def fetch_weather_and_pollution():
    weather_list = []
    pollution_list = []

    print("Start from OpenWeather fetch weather and air pollution data.")

    for city_query in CITIES:
         # 1. Call weather API to get city coordinates (lat, lon)
        weather_url = f"https://api.openweathermap.org/data/2.5/weather?q={city_query}&appid={API_KEY}"
        print(f"Fetching weather data for....: {city_query}")

        try:
            w_res = requests.get(weather_url, timeout=5)
            if w_res.status_code != 200:
                print(f"Weather data fetch failed for {city_query}: {w_res.text}")
                continue

            w_data = w_res.json()
            city_name = w_data["name"]  
            lat = w_data["coord"]["lat"]
            lon = w_data["coord"]["lon"]

            weather_entry = {
                "city": city_name,
                "weather": w_data["weather"][0]["main"],
                "temp_k": w_data["main"]["temp"],
                "dt": w_data["dt"]
            }
            weather_list.append(weather_entry)

            # 2. Call air pollution API 
            pollution_url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}"
            print(f"Fetching air pollution data for {city_name} ({lat}, {lon})")

            p_res = requests.get(pollution_url, timeout=5)
            if p_res.status_code == 200:
                p_data = p_res.json()
                p_main = p_data["list"][0]

                pollution_entry = {
                    "city": city_name,
                    "aqi": p_main["main"]["aqi"],            # 1-5 空氣品質指數
                    "pm2_5": p_main["components"]["pm2_5"],  # PM2.5 濃度
                    "pm10": p_main["components"]["pm10"],    # PM10 濃度
                    "dt": p_main["dt"]                       # 空污觀測時間戳記
                }
                pollution_list.append(pollution_entry)
            else:
                print(f"Air pollution data fetch failed {city_name}: {p_res.text}")
        except requests.exceptions.RequestException as e:
                print(f"🚨 網路連線異常: {e}")

    return weather_list, pollution_list


def save_to_duckdb(weather_data, pollution_data):
    conn = duckdb.connect("local_weather.duckdb")

    # A table: Insert weather data into main.raw_weather_data
    if weather_data:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS main.raw_weather_data (
                city VARCHAR, 
                weather VARCHAR, 
                temp_k DOUBLE, 
                dt BIGINT
            )
        """)
        conn.execute("TRUNCATE main.raw_weather_data")
        for row in weather_data:
            conn.execute("INSERT INTO main.raw_weather_data VALUES (?, ?, ?, ?)", 
                         (row["city"], row["weather"], row["temp_k"], row["dt"]))
        print("Weather data successfully inserted into main.raw_weather_data")

    # B table: Insert pollution data into main.raw_pollution_data
    if pollution_data:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS main.raw_pollution_data (
                city VARCHAR, 
                aqi INTEGER, 
                pm2_5 DOUBLE, 
                pm10 DOUBLE,
                dt BIGINT
            )
        """)
        conn.execute("TRUNCATE main.raw_pollution_data")
        for row in pollution_data:
            conn.execute("INSERT INTO main.raw_pollution_data VALUES (?, ?, ?, ?, ?)", 
                         (row["city"], row["aqi"], row["pm2_5"], row["pm10"], row["dt"]))
        print("Air pollution data successfully inserted into main.raw_pollution_data")

    conn.close()


if __name__ == "__main__":
    weather_res, pollution_res = fetch_weather_and_pollution()
    save_to_duckdb(weather_res, pollution_res)