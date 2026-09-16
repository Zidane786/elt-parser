-- SAIDI/SAIFI reliability indices per reporting period and feeder.
--
-- Source: reg_stg.reliability_events
-- Target: reg_cur.mart_saidi_saifi
-- Owner: regulatory-reporting@utility.example.com
-- Grain: one row per (period, feeder)
-- Schedule: 0 5 1 * *
-- Dependencies: stage_reliability_events
--
-- SAIDI = total customer-interruption minutes / customers served.
-- SAIFI = total customer interruptions / customers served.

CREATE TABLE reg_cur.mart_saidi_saifi AS
SELECT
    e.period                                                    AS period,
    e.feeder_id                                                 AS feeder_id,
    SUM(e.minutes * e.customers_affected)
        / NULLIF(SUM(e.customers_affected), 0)                  AS saidi,
    SUM(e.customers_affected)
        / NULLIF(COUNT(DISTINCT e.outage_id), 0)                AS saifi,
    SUM(e.customers_affected)                                   AS customers
FROM reg_stg.reliability_events AS e
GROUP BY
    e.period,
    e.feeder_id;
