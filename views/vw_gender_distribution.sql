CREATE VIEW vw_gender_distribution AS
SELECT gender, COUNT(*) AS total
FROM dev_HealthConnect_refined.dbo.refined_patients
GROUP BY gender;