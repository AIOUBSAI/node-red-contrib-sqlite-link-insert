# node-red-contrib-sqlite-link-insert

A [Node-RED](https://nodered.org/) node for **grouped, multi-table inserts into SQLite** with powerful features:

- Bulk inserts with transaction modes (`all`, `perTable`, `chunk`, `off`).
- Conflict strategies: `none`, `OR IGNORE`, `OR REPLACE`, `UPSERT`.
- Typed inputs (`msg`, `flow`, `global`, `jsonata`, constants, env).
- KeySpec: build key→id maps for parent-child relationships.
- Lookup mapping: child rows can reference parent IDs by natural keys.
- Optional *returnRows*: re-select affected rows after insert.
- PRAGMAs: enable WAL, synchronous modes, extra pragmas.
- **Config file support**:
  - Save/load to JSON file under `userDir`
  - Lock to file (runtime override)
  - Watch file (hot-reload when changed)

---

## Install

```bash
npm install node-red-contrib-sqlite-link-insert
````

Then restart Node-RED. The node appears under the **storage** category.

---

## Basic Usage

Drag a **sqlite link insert** node into your flow and configure:

1. **Database path** (string or typed input, e.g. `msg.dbFile`).
2. **Transaction mode**, chunk size, and pragmas.
3. **Groups** (one per table):

   * Table name
   * Source array (e.g. `msg.payload`, `msg.users`)
   * Auto-map (copy fields) or custom mapping
   * Conflict strategy (`none`, `ignore`, `replace`, `upsert`)
   * Optional return rows (write back affected IDs)

**Example**

Input message:

```json
{
  "payload": [
    { "id": 1, "name": "Alice" },
    { "id": 2, "name": "Bob" }
  ]
}
```

Config:

* Table: `Users`
* Source: `msg.payload`
* Auto-map: ✓
* Conflict: `upsert` on key `id`

Result:

* Records inserted or updated in `Users`.
* Summary available in `msg.sqlite`.

---

## Config File Support

You can externalize your node configuration to a JSON file under `userDir`.

### Example file (`configs/link-insert.json`)

```json
{
  "txMode": "perTable",
  "chunkSize": 500,
  "continueOnError": true,
  "enableWAL": true,
  "groups": [
    {
      "table": "Users",
      "sourceType": "msg",
      "source": "payload",
      "autoMap": true,
      "conflict": "upsert",
      "upsertKeys": ["id"],
      "updateColumns": ["name"]
    }
  ]
}
```

In the node UI:

* Check **Use config file**
* Path: `configs/link-insert.json`
* Optional: **Lock to file** (runtime always reads this file)
* Optional: **Watch file** (auto reload on changes)

---

## Advanced Examples

### A) Parent → Child with Lookup

**Tables**: `Departments`, `Employees`
**Goal**: insert departments, then employees referencing their department ID.

```json
{
  "departments": [
    { "code": "HR",  "name": "Human Resources" },
    { "code": "ENG", "name": "Engineering" }
  ],
  "employees": [
    { "ext_id": "U001", "name": "Alice", "dept_code": "ENG" },
    { "ext_id": "U002", "name": "Bob",   "dept_code": "HR"  }
  ]
}
```

Groups:

* **Departments**: auto-map, UPSERT by `code`, KeySpec on `code`.
* **Employees**: mapping with `lookup` → `dept_id` from Departments group by `dept_code`.

Effect:

* Department rows inserted/updated.
* Employee rows automatically reference correct `dept_id`.

---

### B) JSONata-driven mapping

**Table**: `Users(email, first_name, last_name, age, is_adult)`
**Input**

```json
{
  "payload": [
    { "email": "  JOHN@EXAMPLE.COM ", "name": { "first": "John", "last": "Doe" }, "age": "29" },
    { "email": "jane@example.com",     "name": { "first": "Jane", "last": "Doe" }, "age": ""  }
  ]
}
```

Mapping:

* `email`: JSONata `lowercase(trim(email))`
* `first_name`: JSONata `name.first`
* `last_name`: JSONata `name.last`
* `age`: path `"age"` with transform `number`
* `is_adult`: JSONata `(number(age) >= 18)` with transform `bool01`

Effect:

* Emails normalized to lowercase.
* Age cast to number/null.
* Boolean flag `is_adult` computed.

---

### C) Many-to-Many with junction table

**Tables**: `Users`, `Roles`, `UserRoles`
**Input**

```json
{
  "users": [ { "email": "alice@example.com" }, { "email": "bob@example.com" } ],
  "roles": [ { "code": "ADMIN" }, { "code": "ANALYST" } ],
  "assignments": [
    { "email": "alice@example.com", "role": "ADMIN"   },
    { "email": "alice@example.com", "role": "ANALYST" },
    { "email": "bob@example.com",   "role": "ANALYST" }
  ]
}
```

Groups:

1. Insert/UPSERT into `Users` (KeySpec on `email`).
2. Insert/UPSERT into `Roles` (KeySpec on `code`).
3. Insert into `UserRoles` with **two lookups**:

   * `user_id` ← from Users map via `email`
   * `role_id` ← from Roles map via `code`

Effect:

* Junction table automatically populated with correct IDs.

---

## Notes on SQLite files

When WAL is enabled (`PRAGMA journal_mode=WAL`):

* `*.db` → main database
* `*.db-wal` → write-ahead log
* `*.db-shm` → shared memory file

These files are **normal and expected**. If you prefer a single `.db` file, disable WAL in the node config.

---

## License

MIT © 2025 AIOUBSAI
