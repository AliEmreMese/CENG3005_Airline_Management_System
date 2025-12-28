import pandas as pd
import mysql.connector

# Veritabanı bağlantısı
db = mysql.connector.connect(
    host="localhost", user="root", password="Aliemre.64", database="airline_management_system"
)
cursor = db.cursor()

# CSV'den sadece gerekli eşleşme sütunlarını oku
CSV_PATH = r"C:\ProgramData\MySQL\MySQL Server 9.4\Uploads\flight_data_2024.csv"
print("CSV okunuyor, eşleşmeler hazırlanıyor...")
df = pd.read_csv(CSV_PATH, sep=';', usecols=['origin', 'origin_city_name'], low_memory=False).drop_duplicates()

# 1. Şehir isimlerini ve ID'lerini veritabanından çek
cursor.execute("SELECT city_id, origin_city_name FROM cities")
city_map = {name: cid for (cid, name) in cursor.fetchall()}

print(f"{len(df)} adet havalimanı eşleşmesi işleniyor...")

# 2. Havalimanlarını şehir ID'leri ile güncelle
for _, row in df.iterrows():
    city_name = row['origin_city_name']
    origin_code = row['origin']
    
    if city_name in city_map:
        cid = city_map[city_name]
        cursor.execute("UPDATE airports SET city_id = %s WHERE origin = %s", (cid, origin_code))

db.commit()
print("BİTTİ! Artık airports tablosu NULL değil.")
db.close()