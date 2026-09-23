# Apache Fluss: Current State and Changelog

What if one table could answer both **“What is the latest value?”** and **“What changed?”**

Apache Fluss primary-key tables keep the current value for each key and expose a changelog for downstream readers. I tested both views with 100,000 customer records.

![Fluss experiment results](assets/fluss-experiment.png)

## Results

| Measurement | Observed |
| --- | ---: |
| Customers in the current table | 100,000 |
| Insert events | 100,000 |
| Update-before events | 10,000 |
| Update-after events | 10,000 |
| Total changelog events | 120,000 |
| Balance mismatches after reconstruction | **0** |

I updated the first 10,000 customers from balance `100` to `200`. Customer `1` returned `200`; untouched customer `50000` returned `100`.

The final check reconstructed each balance from the changelog and compared it with the current table. It found **0 mismatches** in this local run.

## Run the experiment

Start the stack from this directory:

```bash
docker compose up -d
docker compose ps -a
docker compose run --rm sql-client
```

At the `Flink SQL>` prompt, run **one statement at a time**. The interactive client rejects multiple statements pasted as a single input.

```sql
CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'coordinator-server:9123');
```

```sql
USE CATALOG fluss_catalog;
```

```sql
SET 'execution.runtime-mode' = 'batch';
```

```sql
SET 'table.dml-sync' = 'true';
```

```sql
SET 'sql-client.execution.result-mode' = 'tableau';
```

Create a bounded source and the Fluss table:

```sql
CREATE TEMPORARY TABLE ids_100k (id BIGINT) WITH ('connector' = 'datagen', 'number-of-rows' = '100000', 'fields.id.kind' = 'sequence', 'fields.id.start' = '1', 'fields.id.end' = '100000');
```

```sql
CREATE TABLE customers_100k (customer_id BIGINT NOT NULL, name STRING, balance INT, PRIMARY KEY (customer_id) NOT ENFORCED);
```

Load the initial records:

```sql
INSERT INTO customers_100k SELECT id, CONCAT('customer-', CAST(id AS STRING)), 100 FROM ids_100k;
```

Create 10,000 IDs and update those customers through an upsert:

```sql
CREATE TEMPORARY TABLE ids_10k (id BIGINT) WITH ('connector' = 'datagen', 'number-of-rows' = '10000', 'fields.id.kind' = 'sequence', 'fields.id.start' = '1', 'fields.id.end' = '10000');
```

```sql
INSERT INTO customers_100k SELECT id, CONCAT('customer-', CAST(id AS STRING)), 200 FROM ids_10k;
```

Compare current state with change history:

```sql
SELECT COUNT(*) AS current_rows FROM customers_100k;
```

```sql
SELECT _change_type, COUNT(*) AS events FROM customers_100k$changelog GROUP BY _change_type;
```

Check balances reconstructed from the changelog against the current table:

```sql
SELECT COUNT(*) AS mismatched_customers FROM (SELECT customer_id, SUM(CASE WHEN _change_type IN ('insert', 'update_after') THEN balance WHEN _change_type IN ('update_before', 'delete') THEN -balance ELSE 0 END) AS replayed_balance FROM customers_100k$changelog GROUP BY customer_id) r FULL OUTER JOIN customers_100k s ON r.customer_id = s.customer_id WHERE r.customer_id IS NULL OR s.customer_id IS NULL OR r.replayed_balance <> s.balance;
```

**Expected result:** `mismatched_customers = 0`.

## Scope

This verifies current balances against the full changelog for this controlled workload. It does not measure write throughput, consumer recovery, or performance against Kafka and Redis. Running the inserts again against the same table will add more changelog events, so use a fresh table for a repeat run.

## References

- [Apache Fluss quickstart](https://fluss.apache.org/docs/quickstart/flink/)
- [Fluss virtual tables](https://fluss.apache.org/docs/1.0/table-design/virtual-tables/)
