### Docs


```bash
rm -rf .venv
poetry install
# Note: need to install core first because others depend on core
poetry run maturin develop -m arro3-core/Cargo.toml
poetry run maturin develop -m arro3-compute/Cargo.toml
poetry run maturin develop -m arro3-io/Cargo.toml
poetry run mkdocs serve
```
