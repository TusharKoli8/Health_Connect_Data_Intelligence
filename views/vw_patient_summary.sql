CREATE VIEW vw_patient_summary AS
SELECT state_code, COUNT(*) AS total_patients
FROM dev_HealthConnect_refined.dbo.refined_patients
GROUP BY state_code;