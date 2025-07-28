import pandas as pd
import numpy as np
import sqlite3


df = pd.read_csv("./data/raw/AirQualityUCI.csv", sep=";", decimal=",")
df = df.iloc[:, :-2]

df.replace(-200, np.nan, inplace=True)

df["Time"] = df["Time"].str.replace(".", ":", regex=False)
df["Datetime"] = pd.to_datetime(df["Date"] + " " + df["Time"], dayfirst=True)
df.drop(["Date", "Time"], axis=1, inplace=True)

conn = sqlite3.connect("./data/processed/air_quality.db")
df.to_sql("raw_readings", conn, if_exists="replace", index=False)
conn.execute("CREATE INDEX IF NOT EXISTS idx_datetime ON raw_readings(Datetime);")
conn.close()

print("Data ingested into SQLite database.")
