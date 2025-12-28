-- backend_logic.sql

USE airline_management_system;

-- 1. Flight Summary View
DROP VIEW IF EXISTS flight_summary_view;
CREATE VIEW flight_summary_view AS
SELECT 
    f.flight_id, 
    c.op_unique_carrier AS carrier,
    ci.origin_city_name AS city,
    s.origin_state_nm AS state,
    can.cancelled
FROM flights f
JOIN carriers c ON f.op_unique_carrier = c.op_unique_carrier
JOIN airports a ON f.origin = a.origin
JOIN cities ci ON a.city_id = ci.city_id
JOIN states s ON ci.state_id = s.state_id
LEFT JOIN cancellations can ON f.flight_id = can.flight_id;

-- 2. Analytical Stored Procedure (IN/OUT Parameters)
DELIMITER //

CREATE PROCEDURE GetStateTotalDelay(
    IN state_name_param VARCHAR(100),
    OUT total_delay_output FLOAT
)
BEGIN
    SELECT SUM(m.metric_value) INTO total_delay_output
    FROM flight_metrics m
    JOIN flights f ON m.flight_id = f.flight_id
    JOIN airports a ON f.origin = a.origin
    JOIN cities ci ON a.city_id = ci.city_id
    JOIN states s ON ci.state_id = s.state_id
    WHERE s.origin_state_nm = state_name_param 
      AND m.metric_name = 'dep_delay';
END //

DELIMITER ;