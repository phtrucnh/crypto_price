-- ============================
-- DROP ALL FOREIGN KEYS
-- ============================
DECLARE @sql NVARCHAR(MAX) = N'';

SELECT @sql = STRING_AGG(
    N'ALTER TABLE ' + QUOTENAME(SCHEMA_NAME(fk.schema_id)) + N'.' + QUOTENAME(OBJECT_NAME(fk.parent_object_id)) +
    N' DROP CONSTRAINT ' + QUOTENAME(fk.name) + N';', CHAR(10)
)
FROM sys.foreign_keys AS fk;

IF @sql IS NOT NULL AND LEN(@sql) > 0
BEGIN
    PRINT @sql;  -- for visibility
    EXEC sp_executesql @sql;
END

-- ============================
-- (Optional) HANDLE TEMPORAL TABLES
-- If you have system-versioned (temporal) tables, turn off versioning first.
-- ============================
SET @sql = N'';
SELECT @sql = STRING_AGG(
    N'ALTER TABLE ' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + N'.' + QUOTENAME(t.name) +
    N' SET (SYSTEM_VERSIONING = OFF);', CHAR(10)
)
FROM sys.tables t
WHERE t.temporal_type = 2; -- 2 = SYSTEM_VERSIONED_TEMPORAL_TABLE

IF @sql IS NOT NULL AND LEN(@sql) > 0
BEGIN
    PRINT @sql;
    EXEC sp_executesql @sql;
END

-- ============================
-- DROP ALL TABLES
-- ============================
SET @sql = N'';
SELECT @sql = STRING_AGG(
    N'DROP TABLE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) + N';', CHAR(10)
)
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
-- Exclude system tables (none appear in sys.tables) and (optionally) any you want to keep:
-- WHERE s.name + '.' + t.name NOT IN ('dbo.__EFMigrationsHistory')

IF @sql IS NOT NULL AND LEN(@sql) > 0
BEGIN
    PRINT @sql;
    EXEC sp_executesql @sql;
END
