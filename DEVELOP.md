## Docs

```bash
rm -rf .venv
uv sync
# Note: need to install core first because others depend on core
uv run maturin dev -m arro3-core/Cargo.toml
uv run maturin dev -m arro3-compute/Cargo.toml
uv run maturin dev -m arro3-io/Cargo.toml
uv run mkdocs serve
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

Install maturin and pyodide-build (choose a specific version of pyodide-build if desired)

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

## Updating pyo3 version

It takes a few steps to update for a new pyo3 version. We have to do these steps in a specific order because of intertangled dependencies.

arro3 depends on pyo3-arrow and pyo3-object_store. pyo3-object_store itself depends on pyo3-arrow, so we have to wait for a pyo3-arrow release before we can update pyo3-object_store, and then we need a `pyo3-object_store` release before we can update arro3 itself.

1. For simplicity, arro3 often uses the workspace dependency version of `pyo3-arrow`, with `pyo3-arrow = { path = "./pyo3-arrow" }` in `Cargo.toml`. Instead, we need to unlink this so that we can update arro3.

    Change this to use the git dependency instead, pointing to the latest commit hash of `pyo3-arrow` that uses the existing pyo3 version. I.e. something like this

    ```toml
    pyo3-arrow = { git = "https://github.com/kylebarron/arro3", rev = "e2de4da2f732667dd796335ea7def8b111f79838" }
    ```
    Where this `rev` is the current `main`.

2. Update pyo3 version in `pyo3-arrow/Cargo.toml`. Open a PR and release a new version of `pyo3-arrow`. Note that `pyo3-arrow` also depends on `numpy`, so we need to wait for a new release there, but that should be pretty fast.
3. Ensure `pyo3-async-runtimes` and `pyo3-file` have releases for latest `pyo3`.
4. Update `pyo3-object-store` to depend on the new `pyo3-arrow` version. Open a PR and release a new version of `pyo3-object-store`.
5. Finally, update `arro3/Cargo.toml` to depend on the new `pyo3-arrow` and `pyo3-object-store` versions, and update pyo3 version. Open a PR and release a new version of `arro3`.

## Publishing to crates.io

To publish to crates.io, we have Github Actions workflows set up to publish when pushing tags with specific formats.

- To publish `pyo3-arrow`, push a tag like `pyo3-arrow-vX.Y.Z` where `X.Y.Z` is the new version.
- To publish `pyo3-bytes`, push a tag like `pyo3-bytes-vX.Y.Z` where `X.Y.Z` is the new version.
