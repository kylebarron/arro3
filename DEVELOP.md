## Docs

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

## Emscripten Python wheels

Install rust nightly and add wasm toolchain

```bash
rustup toolchain install nightly
rustup target add --toolchain nightly wasm32-unknown-emscripten
```

Install maturin and pyodide-build

```bash
pip install -U maturin
pip install pyodide-build
```

Clone emsdk. I clone this into a specific path at `~/github/emscripten-core/emsdk` so that it can be shared across projects.

```bash
mkdir -p ~/github/emscripten-core/
git clone https://github.com/emscripten-core/emsdk.git ~/github/emscripten-core/emsdk
# Or, set this manually
PYODIDE_EMSCRIPTEN_VERSION=$(pyodide config get emscripten_version)
~/github/emscripten-core/emsdk/emsdk install ${PYODIDE_EMSCRIPTEN_VERSION}
~/github/emscripten-core/emsdk/emsdk activate ${PYODIDE_EMSCRIPTEN_VERSION}
source ~/github/emscripten-core/emsdk/emsdk_env.sh
```

Build `arro3-core`:

```bash
RUSTUP_TOOLCHAIN=nightly \
    maturin build \
    --release \
    -o dist \
    -m arro3-core/Cargo.toml \
    --target wasm32-unknown-emscripten \
    -i python3.11
```
