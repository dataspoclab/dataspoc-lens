# Contributing Rules — DataSpoc Lens

Rules for maintaining the project vision. Use this to evaluate PRs.

---

## 1. Scope Rules

**Lens does ONE thing: let users query data in cloud buckets with SQL.**

| ACCEPT | REJECT |
|--------|--------|
| New DuckDB query optimization | Data ingestion features (that's Pipe) |
| New shell dot command | Writing to raw/ in the bucket |
| Better autocomplete | ML/training features (that's ML) |
| New export format | Data pipeline orchestration |
| Cache improvement | User authentication/billing |
| AI prompt improvement | Custom database engine |
| Jupyter integration fix | Streaming/real-time queries |
| New cloud provider support | Multi-tenancy/RBAC |
| Transform SQL improvement | Web UI (this is CLI-first) |

**The golden question:** "Does this help users query their data easier?" If no, reject.

---

## 2. Architecture Rules

### Read-Only on Bucket

Lens **never writes to `raw/`** in the bucket. That's Pipe's job.

Lens CAN write to:
- `curated/` — via transforms (`.sql` files)
- Local cache (`~/.dataspoc-lens/cache/`)

### Never Implement Auth — IAM Handles It

DataSpoc does not implement authentication, authorization, or RBAC. Cloud provider IAM (AWS IAM, GCP IAM, Azure AD) controls who can access which buckets. Lens needs only READ access. Each analyst's cloud credentials determine which buckets they can see — if they lack permission, `add-bucket` fails with "Access Denied" and they see nothing. Any PR that adds user management, access control lists, or permission checks must be rejected.

### Manifest Compatibility

Lens must read manifests written by Pipe. Support both formats:
- Dict format: `{"tables": {"source/table": {...}}}`
- List format: `{"tables": [{...}, ...]}`

Never reject a valid manifest. Be lenient in parsing, strict in output.

### DuckDB is the Engine

- All queries go through DuckDB
- Views use `read_parquet()` with `hive_partitioning=true, union_by_name=true`
- No custom query parser — let DuckDB handle SQL
- DuckDB extensions (httpfs) loaded at connection time

### Cache is Optional

- Everything must work WITHOUT cache (direct from cloud)
- Cache is a performance optimization, not a requirement
- `mount_views()` auto-detects fresh cache and uses it — but never fails if cache is missing

---

## 3. Code Rules

### Must Have
- Tests for every new feature (`pytest`)
- Shell dot commands documented in `.help`
- AI prompts include table DDL + sample data
- Export functions work with all formats (CSV, JSON, Parquet)
- `import duckdb` only in modules that need it (not at top of cli.py)

### Must NOT Have
- No hardcoded cloud credentials
- No `print()` — use `console.print()` (Rich)
- No blocking downloads without progress indicator
- No silent failures in catalog discovery — warn user
- No Python < 3.10 syntax
- No mandatory heavy deps in core (jupyter, anthropic, openai must be optional extras)

### Style
- CLI messages in English
- Function/variable names in English, snake_case
- Type hints on function signatures
- Keep files under 300 lines

---

## 4. PR Checklist

Before approving any PR:

- [ ] Tests pass (`pytest tests/ -v`)
- [ ] Works with `file://` (local) and `s3://` (cloud)
- [ ] Manifest compatibility not broken (dict + list formats)
- [ ] Shell dot commands updated if new features
- [ ] Optional deps not added to core requirements
- [ ] Cache behavior tested (with and without cache)
- [ ] AI features gracefully handle missing API key
- [ ] Jupyter notebook template updated if tables change

---

## 5. Dependency Rules

- **Core deps** (duckdb, typer, prompt-toolkit, fsspec, rich, tabulate): update freely
- **Cloud backends** (s3fs, gcsfs, adlfs): always optional `[s3]`, `[gcs]`, `[azure]`
- **Jupyter** (jupyterlab, jupysql): always optional `[jupyter]`
- **AI** (anthropic, openai): always optional `[ai]`
- **New core deps**: must justify. Heavy deps go in extras
- **pyarrow**: dev dependency only (for cache tests), not in core

---

## 6. AI Rules

- User provides their own API key — Lens never ships with keys
- Prompts include DDL of ALL tables + sample data (LIMIT 3)
- Generated SQL is shown to user BEFORE execution
- If LLM fails, show clear message — never crash
- Support both Anthropic and OpenAI — provider via config
- `--debug` flag shows full prompt sent to LLM

---

## 7. Release Rules

- Semantic versioning: `MAJOR.MINOR.PATCH`
- Breaking changes to CLI commands: MAJOR bump
- New features: MINOR bump
- Bug fixes: PATCH bump
- Test on `file://` and at least one cloud provider before release
- Jupyter notebook template must be tested
