# Fixture CLI reference

## `tool ingest`

Hand-written purpose stays.

<!-- generated:cli:ingest -->
| Option | What it does | Default |
|---|---|---|
| `paths` | files or folders to read | |
| `--profile {health,all}` | signals to export: health or all \| about 3x larger | `health` |
| `--fast` | skip the slow part | |
<!-- /generated:cli:ingest -->

```sh
tool ingest logs/
```

## `tool doctor`

<!-- generated:cli:doctor -->
| Option | What it does | Default |
|---|---|---|
| `--lake LAKE` | lake root | |
<!-- /generated:cli:doctor -->
