
--==================
-- AZURE SQL - Database
--=================

-- Device Table

CREATE TABLE Device
  (
    DId		   INT IDENTITY(1,1),
    DeviceId   UNIQUEIDENTIFIER PRIMARY KEY,
    DeviceName NVARCHAR(255)    NOT NULL,
    CreatedAt  DATETIME         NOT NULL
  );


-- DeviceReading Table

CREATE TABLE DeviceReading
  (
    [DId]			   INT IDENTITY(1,1),
    [DeviceId]         UNIQUEIDENTIFIER NOT NULL,
    [CurrentValue]     FLOAT            NOT NULL,
    [Unit]             VARCHAR(50)      NOT NULL,
    [ReadingTimestamp] DATETIMEOFFSET   NOT NULL,
    [Version]          FLOAT            NOT NULL,
    FOREIGN KEY (DeviceId) REFERENCES Device(DeviceId)
  );


  --================
  -- QUERY PLAN (Database)
  --================

  --Creating Index

  CREATE INDEX idx_deviceId_timestamp ON DeviceReading (DeviceId, ReadingTimestamp);

  -- Data Retrieval Query

  WITH LatestReadings AS (
    SELECT DeviceId, MAX(ReadingTimestamp) AS ReadingTimestamp
    FROM DeviceReading
    GROUP BY DeviceId
)
SELECT 
    dr.DeviceId,
    d.DeviceName,
    dr.CurrentValue,
    dr.Unit,
    dr.ReadingTimestamp
FROM DeviceReading dr
JOIN Device d ON dr.DeviceId = d.DeviceId 
JOIN LatestReadings lr ON dr.DeviceId = lr.DeviceId AND dr.ReadingTimestamp = lr.ReadingTimestamp;

  --================
-- Azure Synapse - DW
  --================

-- Create Schema

CREATE SCHEMA DW;

--Device Dimension Table

CREATE TABLE DW.d_Device
  (
    [SK]          INT              IDENTITY (1, 1),
    [DeviceId]    UNIQUEIDENTIFIER NOT NULL UNIQUE,
    [DeviceName]  NVARCHAR(255)    NOT NULL,
    [CreatedAt]   DATETIMEOFFSET   NOT NULL,
    [ETLLoadDate] DATETIME         NOT NULL,
    [UpdatedAt]   DATETIME         NULL
  );  

  --DeviceRading Fact Table

CREATE TABLE DW.f_DeviceReading
  (
    [SK]               INT            IDENTITY (1, 1),
    [DeviceSK]         INT            NOT NULL,
    [CurrentValue]     FLOAT          NOT NULL,
    [Unit]             VARCHAR(50)    NOT NULL,
    [ReadingTimestamp] DATETIMEOFFSET NOT NULL,
    [Version]          FLOAT          NOT NULL,
    [ETLLoadDate]      DATETIME       NOT NULL,
    [UpdatedAt]        DATETIME       NULL
  );

  --================
  -- QUERY PLAN (DW)
  --================

  -- Creating Index

CREATE CLUSTERED INDEX IDX_DeviceSK ON DW.f_DeviceReading (DeviceSK);

CREATE CLUSTERED INDEX IX_d_Device_SK ON DW.d_Device(SK);

-- Data Retrieval Query

SELECT 
    d.DeviceName, 
    dr.CurrentValue, 
    dr.ReadingTimestamp,
    dr.Unit
FROM 
   DW.f_DeviceReading dr
JOIN 
   DW.d_Device d ON d.SK = dr.DeviceSK ;



