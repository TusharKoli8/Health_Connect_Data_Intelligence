CREATE VIEW vw_revenue_summary AS
SELECT 
    claim_status,
    COUNT(*) AS total_claims,
    SUM(total_billed_amount) AS total_billed,
    SUM(total_allowed_amount) AS total_allowed,
    SUM(total_paid_amount) AS total_paid,
    (SUM(total_billed_amount) - SUM(total_paid_amount)) AS revenue_gap
FROM dev_HealthConnect_refined.dbo.refined_claims
GROUP BY claim_status;

