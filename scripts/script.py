import pandas as pd
import mysql.connector

# 1. AYARLAR
CSV_PATH = r"C:\ProgramData\MySQL\MySQL Server 9.4\Uploads\flight_data_2024.csv"

db_config = {
    "host": "localhost",
    "user": "root",
    "password": "Aliemre.64", # Buraya kendi MySQL şifreni yaz
    "database": "airline_management_system"
}

# RDF tablosuna satır olarak eklenecek metrikler listesi
METRIC_COLUMNS = [
    'dep_delay', 'arr_delay', 'taxi_out', 'taxi_in', 'wheels_off', 
    'wheels_on', 'air_time', 'actual_elapsed_time', 'distance', 
    'carrier_delay', 'weather_delay', 'nas_delay', 'security_delay', 
    'late_aircraft_delay', 'diverted', 'year', 'month', 'day_of_month',
    'day_of_week', 'crs_dep_time', 'dep_time', 'crs_arr_time', 'arr_time'
]

def run_etl():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        print(">>> Bağlantı başarılı. Noktalı virgül ayracıyla veri okunuyor...")

        # KRİTİK DEĞİŞİKLİK: sep=';' eklendi
        for chunk in pd.read_csv(CSV_PATH, sep=';', chunksize=50000, low_memory=False):
            # Sütun isimlerini her ihtimale karşı temizle
            chunk.columns = chunk.columns.str.strip().str.lower()
            
            # --- A. MASTER TABLOLARI DOLDUR (Unique) ---
            for val in chunk['origin_state_nm'].dropna().unique():
                cursor.execute("INSERT IGNORE INTO states (origin_state_nm) VALUES (%s)", (val,))
            
            for val in chunk['origin_city_name'].dropna().unique():
                cursor.execute("INSERT IGNORE INTO cities (origin_city_name) VALUES (%s)", (val,))

            for val in chunk['op_unique_carrier'].dropna().unique():
                cursor.execute("INSERT IGNORE INTO carriers (op_unique_carrier) VALUES (%s)", (val,))

            for val in chunk['origin'].dropna().unique():
                cursor.execute("INSERT IGNORE INTO airports (origin) VALUES (%s)", (val,))

            # --- B. FLIGHTS & METRICS DOLDUR ---
            for _, row in chunk.iterrows():
                # 1. Flights
                cursor.execute("""
                    INSERT INTO flights (op_unique_carrier, op_carrier_fl_num, origin, dest) 
                    VALUES (%s, %s, %s, %s)""", 
                    (row['op_unique_carrier'], row['op_carrier_fl_num'], row['origin'], row['dest']))
                
                flight_id = cursor.lastrowid # Yeni oluşan flight_id

                # 2. Cancellations (fl_date burada)
                try:
                    # Tarihi MySQL formatına (YYYY-MM-DD) çevir
                    clean_date = pd.to_datetime(row['fl_date']).strftime('%Y-%m-%d')
                    cursor.execute("""
                        INSERT INTO cancellations (flight_id, fl_date, cancelled, cancellation_code) 
                        VALUES (%s, %s, %s, %s)""", 
                        (flight_id, clean_date, row['cancelled'], row['cancellation_code']))
                except Exception as e:
                    pass # Tarih hatası varsa atla

                # 3. Flight Metrics (RDF Mantığı - 35 sütun kuralı burada sağlanıyor)
                for m_name in METRIC_COLUMNS:
                    if m_name in row and pd.notnull(row[m_name]):
                        cursor.execute("""
                            INSERT INTO flight_metrics (flight_id, metric_name, metric_value) 
                            VALUES (%s, %s, %s)""", (flight_id, m_name, float(row[m_name])))

            conn.commit()
            print(">>> 50.000 satır başarıyla dağıtıldı.")

        print("\n[BİTTİ] 1 Milyon satır 7 tabloya RDF yapısında yüklendi!")

    except Exception as e:
        print(f"\n[HATA] İşlem sırasında bir sorun oluştu: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    run_etl()