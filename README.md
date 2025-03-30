# Simple Optimized Sort Merge Join in Rust

This project ports the database assignment from C++ to Rust. It implements an optimized sort-merge join algorithm designed to efficiently process large datasets.

- To see the result please check the `Join.csv` file.
- One employee can have multiple departments.
- Departments' `manager_id` and employees' `id` are used as the join keys.

Based on the template code, a few improvements have been made:
- Follows SOLID object-oriented principles as closely as possible.
- Incorporates strategy pattern for sorting and writing records.

## How to Run

```bash
cargo run
```

