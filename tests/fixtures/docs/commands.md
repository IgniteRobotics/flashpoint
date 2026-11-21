# Commands

```sh
flashpoint ingest logs/
flashpoint ingest --recursive logs/
FLASHPOINT_CONFIG=~/team-config flashpoint doctor
poetry run flashpoint serve --port 8001
uv run flashpoint report --static <usb-folder>
flashpoint init --team <your team number>  # a trailing comment
flashpoint acquire \
  --dry-run \
  --no-usb
uv tool install --python 3.13 flashpoint
# flashpoint is mentioned in a comment
flashpoint serve --colour red
```

```console
$ flashpoint backup --lake ~/lake
backed up 3 files
$ flashpoint frobnicate
```

```powershell
$env:FLASHPOINT_LAKE = "D:\lake"; flashpoint ingest "$HOME\Downloads\GACMP"
flashpoint report --match <match-key> --no-raw --bogus
```

```toml
flashpoint = "not a command"
```
