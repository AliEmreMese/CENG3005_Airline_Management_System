import mysql.connector

# 1. BAĞLANTI AYARLARI
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Aliemre.64", # Kendi şifreni yaz
    database="airline_management_system"
)
cursor = db.cursor()

# 2. EYALET SÖZLÜĞÜ (Kısaltma -> Tam İsim)
state_map = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
    'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
    'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri',
    'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
    'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio',
    'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
    'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming',
    'PR': 'Puerto Rico', 'VI': 'Virgin Islands'
}

print("Eşleştirme işlemi başlıyor...")

# 3. ŞEHİRLERİ TEK TEK GÜNCELLE
for abbr, full_name in state_map.items():
    # States tablosundaki state_id'yi al
    cursor.execute("SELECT state_id FROM states WHERE origin_state_nm = %s", (full_name,))
    result = cursor.fetchone()
    
    if result:
        state_id = result[0]
        # Şehir isminin sonu bu kısaltmayla bitenleri güncelle
        sql = "UPDATE cities SET state_id = %s WHERE origin_city_name LIKE %s"
        cursor.execute(sql, (state_id, f'%, {abbr}'))

db.commit()
print("İşlem bitti! Artık state_id sütunları dolu olmalı.")
db.close()