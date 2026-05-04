SELECT COUNT(*) FROM storm_events;

SELECT event_type, S, severity
FROM storm_events
ORDER BY S DESC
LIMIT 10;

SELECT state, COUNT(*)
FROM storm_events
GROUP BY state
ORDER BY COUNT(*) DESC
LIMIT 10;