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

### Adding a new module

- Add new module to Github Actions matrix in `wheels.yml`
- Update `docs.yml` to include module
