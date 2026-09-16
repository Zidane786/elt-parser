-- Energy sold + invoiced revenue per reporting period (regulatory filing).
--
-- Source: bill_cur.fact_invoice, meter_cur.fact_consumption
-- Target: reg_cur.mart_energy_sold
-- Owner: regulatory-reporting@utility.example.com
-- Grain: one row per reporting period
-- Schedule: 0 5 1 * *
-- Dependencies: none
--
-- Joins billed invoices to metered consumption on the shared account_id so the
-- filing reconciles billed revenue against delivered kWh.

CREATE TABLE reg_cur.mart_energy_sold AS
SELECT
    date_format(i.issue_date, '%Y') || '-Q'
        || CAST(quarter(i.issue_date) AS VARCHAR)               AS period,
    SUM(c.kwh)                                                  AS total_kwh,
    SUM(i.amount)                                               AS total_revenue
FROM bill_cur.fact_invoice AS i
JOIN meter_cur.fact_consumption AS c
    ON i.account_id = c.account_id
GROUP BY
    date_format(i.issue_date, '%Y') || '-Q'
        || CAST(quarter(i.issue_date) AS VARCHAR);
