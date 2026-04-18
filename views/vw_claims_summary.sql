CREATE VIEW vw_claims_summary AS
SELECT claim_status, COUNT(*) AS total_claims,
       SUM(total_paid_amount) AS total_revenue
FROM dev_HealthConnect_refined.dbo.refined_claims
GROUP BY claim_status;