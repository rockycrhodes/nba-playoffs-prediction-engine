# Data Quality Checks

Each `.sql` file returns only "bad rows".  
If a query returns 0 rows, that check passes.

Recommended order:
1. dq_01_orphans.sql
2. dq_02_duplicates.sql
3. dq_03_completeness.sql
4. dq_04_ranges.sql
5. dq_05_cross_table_consistency.sql
