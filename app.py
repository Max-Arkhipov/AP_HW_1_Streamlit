import streamlit as st
import pandas as pd
import requests
import numpy as np
from datetime import date
from multiprocessing import Pool
import plotly.express as px

st.title("Анализ температур в городах и поиск аномалий с использованием Streamlit")

st.header("Шаг 1: Загрузка данных")

uploaded_file = st.file_uploader("Выберите CSV-файл", type=["csv"])

# Функция для обработки одной части DataFrame
def process_chunk(chuck):
    chuck["roll_mean"] = chuck["temperature"].rolling(window=30, center=True).mean()
    chuck["mean_season"] = chuck["roll_mean"]
    chuck["mean_season"] = chuck.groupby(["season"])["mean_season"].transform('mean')
    chuck["std_season"] = chuck["temperature"] - chuck["roll_mean"]
    chuck["std_season"] = chuck.groupby(["season"])["std_season"].transform('std')
    chuck["is_anomaly"] = abs(chuck["temperature"] - chuck["mean_season"]) > 2 * chuck["std_season"]
    return chuck

# Разделение DataFrame на части
def parallel_apply(df, func):
    cities = df['city'].unique()
    chunks = [df[df['city'] == city] for city in cities]
    with Pool(6) as pool:
        results = pool.map(func, chunks)
    return pd.concat(results)

@st.cache_data
def load_data(file):
    return pd.read_csv(file)

if uploaded_file is not None:
    data = load_data(uploaded_file)
    data['timestamp'] = data['timestamp'].astype('datetime64[ns]')
    # data['timestamp'] = pd.to_date(data['timestamp'])
    data = parallel_apply(data, process_chunk)
    st.write("Превью обработанных данных:")
    st.dataframe(data)
else:
    st.write("Пожалуйста, загрузите CSV-файл.")

if uploaded_file is not None:
    st.header("Шаг 2. Выберите город")
    city = st.selectbox("Выберите город для отображения", options=np.sort(data['city'].unique()))

def test_key(API_KEY, city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        st.write("Корректный API-ключ OpenWeatherMap")
    elif response.status_code == 401:
        st.write(
            '{"cod": 401, "message": "Invalid API key. Please see https://openweathermap.org/faq#error401 for more info."}')
    else:
        st.write('При проверке ключа возникла ошибка')

if uploaded_file is not None:
    data_city = data[data['city'] == city]
    st.dataframe(data[data['city'] == city].describe(include='all'))
    st.write(f"Период наблюдений: {min(data_city['timestamp']).date()} - {max(data_city['timestamp']).date()}")
    st.write(f"Минимальная температура за период наблюдений: {round(min(data_city['temperature']), 2)}")
    st.write(f"Максимальная температура за период наблюдений: {round(max(data_city['temperature']), 2)}")
    st.write(f"Число дней с аномальной температурой за период наблюдений: {data_city[data_city['is_anomaly'] == True].shape[0]}")

if uploaded_file is not None:
    st.header(f"График температур {city}")
    fig = px.scatter(data_city,
                     x=data_city.columns[1],
                     y=data_city.columns[2],
                     color_discrete_sequence=["blue", "red"],
                     color=data_city.columns[7])
    st.plotly_chart(fig)

if uploaded_file is not None:
    st.header("Распределение аномалий по сезонам")
    data_seasons_anomaly = data_city[data_city['is_anomaly'] == True]['season'].value_counts().reset_index()
    fig = px.pie(data_seasons_anomaly, values='count', names='season')
    st.plotly_chart(fig)

if uploaded_file is not None:
    seasons = data['season'].unique()
    st.write(city)
    for season in seasons:
        record = data[(data["city"] == city) & (data["season"] == season)].iloc[0, :]
        st.write(f"{record['season']}:")
        st.write(f"Средняя температура: {round(record['mean_season'], 2)}")
        st.write(f"Стандартное отклонение: {round(record['std_season'], 2)}")

if uploaded_file is not None:
    st.header("Шаг 3. Введите API-ключ OpenWeatherMap")
    API_KEY = st.text_input("Введите ключ")
    if st.button("Проверить ключ"):
        test_key(API_KEY, city)

def is_anomaly_cor(temp, city):
    month_to_season = {12: "winter", 1: "winter", 2: "winter",
                       3: "spring", 4: "spring", 5: "spring",
                       6: "summer", 7: "summer", 8: "summer",
                       9: "autumn", 10: "autumn", 11: "autumn"}

    season = month_to_season[date.today().month]
    record = data[(data["city"] == city) & (data["season"] == season)].iloc[0, :]
    anomaly = abs(temp - record["mean_season"]) > 2 * record["std_season"]

    return f'''Город: {city}  
                Текущая температура: {temp}  
                Аномальная: {anomaly}'''

def get_temp(API_KEY, city):
  url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
  response = requests.get(url)
  if response.status_code == 200:
      data_weat = response.json()
      temp = data_weat['main']['temp']
  elif response.status_code == 401:
      return f'("cod":401, "message": "Invalid API key. Please see https://openweathermap.org/faq#error401 for more info.")'
  else:
      return f'город: {city} Ошибка при запросе данных'
  return is_anomaly_cor(temp, city)

if uploaded_file is not None:
    st.header("Шаг 4. Просмотр текущего значения")
    if st.button("Показать текущую температуру"):
        if API_KEY:
            st.write(get_temp(API_KEY, city))
        else:
            st.write("Для просмотра текущей температуры введите API key")
