SELECT
    n.nspname AS schema_name,
    p.typname AS type_name,
    CASE
        WHEN p.typtype = 'e' THEN 'ENUM'
        WHEN p.typtype = 'c' THEN 'COMPOSITE'
        WHEN p.typtype = 'b' THEN 'BASE'
        ELSE 'OTHER'
    END AS type_category
FROM
    pg_type p
JOIN
    pg_namespace n ON n.oid = p.typnamespace
WHERE
    n.nspname NOT IN ('pg_catalog', 'information_schema')  -- Exclude system types
    AND p.typtype IN ('e', 'c')  -- Only include enums and composite types
ORDER BY
    schema_name, type_name;
