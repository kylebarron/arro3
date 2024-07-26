### Docs


```bash
rm -rf .venv
poetry install
poetry run maturin build -m arro3-compute/Cargo.toml -o dist
poetry run maturin build -m arro3-core/Cargo.toml -o dist
poetry run maturin build -m arro3-io/Cargo.toml -o dist
poetry run pip install dist/*
poetry run mkdocs serve
```
