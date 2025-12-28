-- analytical_queries.sql

USE airline_management_system;

-- Sorgu 1: Her eyaletteki toplam uçuş sayısını bul (Sadece NY olmadığını kanıtlar)
SELECT state, COUNT(*) as flight_count
FROM flight_summary_view
GROUP BY state
ORDER BY flight_count DESC;

-- Sorgu 2: En fazla rötar yapan ilk 5 havayolu şirketini ve ortalama rötarlarını bul
SELECT 
    f.op_unique_carrier AS carrier_code, 
    AVG(m.metric_value) AS avg_departure_delay
FROM flights f
JOIN flight_metrics m ON f.flight_id = m.flight_id
WHERE m.metric_name = 'dep_delay'
GROUP BY f.op_unique_carrier
ORDER BY avg_departure_delay DESC
LIMIT 5;

-- Sorgu 3: En uzun mesafeli (distance) ilk 10 uçuşu ve kalkış şehirlerini listele
SELECT 
    f.flight_id, 
    ci.origin_city_name, 
    m.metric_value AS distance_miles
FROM flights f
JOIN flight_metrics m ON f.flight_id = m.flight_id
JOIN airports a ON f.origin = a.origin
JOIN cities ci ON a.city_id = ci.city_id
WHERE m.metric_name = 'distance'
ORDER BY distance_miles DESC
LIMIT 10;

-- Sorgu 4: İptal edilen uçuşların (Cancelled) eyalet bazlı dağılımı
SELECT 
    s.origin_state_nm AS state, 
    COUNT(*) AS total_cancellations
FROM cancellations can
JOIN flights f ON can.flight_id = f.flight_id
JOIN airports a ON f.origin = a.origin
JOIN cities ci ON a.city_id = ci.city_id
JOIN states s ON ci.state_id = s.state_id
WHERE can.cancelled = 1
GROUP BY s.origin_state_nm
ORDER BY total_cancellations DESC;