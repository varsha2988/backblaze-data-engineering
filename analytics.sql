-- AFR by model
SELECT model, SUM(drive_days) drive_days, SUM(failure_count) failure_count,
       SUM(failure_count)/NULLIF(SUM(drive_days),0)*365.25*100 afr_percent
FROM daily_model_stats GROUP BY model ORDER BY afr_percent;

-- Top 10 reliable
SELECT * FROM (
 SELECT model, SUM(drive_days) drive_days, SUM(failure_count) failure_count,
        SUM(failure_count)/NULLIF(SUM(drive_days),0)*365.25*100 afr_percent
 FROM daily_model_stats GROUP BY model
) ORDER BY afr_percent ASC LIMIT 10;

-- Top 10 least reliable
SELECT * FROM (
 SELECT model, SUM(drive_days) drive_days, SUM(failure_count) failure_count,
        SUM(failure_count)/NULLIF(SUM(drive_days),0)*365.25*100 afr_percent
 FROM daily_model_stats GROUP BY model
) ORDER BY afr_percent DESC LIMIT 10;

-- Monthly trend
SELECT date_format(drive_date,'%Y-%m') month,
       SUM(drive_days) drive_days, SUM(failure_count) failure_count
FROM daily_model_stats GROUP BY date_format(drive_date,'%Y-%m') ORDER BY month;
