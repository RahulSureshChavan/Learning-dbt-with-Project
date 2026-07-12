WITH dedup_query AS
(
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updateDATE DESC) AS deduplication_id
    FROM
        {{ source('source','items') }}
)
SELECT
    id,
    name,
    category,
    updateDATE
FROM
    dedup_query
WHERE
    deduplication_id = 1