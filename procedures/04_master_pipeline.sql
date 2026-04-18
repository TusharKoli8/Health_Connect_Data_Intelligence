USE dev_HealthConnect_raw;
GO

DROP PROCEDURE IF EXISTS Proc_HealthConnect_Master_Pipeline;
GO

CREATE PROCEDURE Proc_HealthConnect_Master_Pipeline
AS
BEGIN
    EXEC Proc_HealthConnect_Source_To_Raw;
    EXEC Proc_HealthConnect_Raw_To_Cleansed;
    EXEC Proc_HealthConnect_Cleansed_To_Refined;
END;
GO

EXEC Proc_HealthConnect_Master_Pipeline;