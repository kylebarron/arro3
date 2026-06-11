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

Emscripten wheels (PEP 783) are built once per Python version. The entire
toolchain config (Rust toolchain, Emscripten version, ABI tag, rustflags) is
defined by `pyodide-build` *running under that same Python version* — e.g.
Python 3.13 maps to ABI `2025_0`/Emscripten 4.0.9 while Python 3.14 maps to
ABI `2026_0`/Emscripten 5.0.3. Use `uvx -p` to query the config for a given
Python version without touching the project venv:

```bash
PYTHON_VERSION=3.14  # or 3.13
# The `pyodide` executable lives in pyodide-cli; most subcommands (config,
# xbuildenv) are plugins provided by pyodide-build, so both packages are
# needed.
pyodide_cmd() {
    uvx -p "$PYTHON_VERSION" --from pyodide-cli --with pyodide-build pyodide "$@"
}
RUST_TOOLCHAIN=$(pyodide_cmd config get rust_toolchain)
PYODIDE_ABI_VERSION=$(pyodide_cmd config get pyodide_abi_version)
PYODIDE_RUSTFLAGS=$(pyodide_cmd config get rustflags)
PYODIDE_CFLAGS=$(pyodide_cmd config get cflags)

echo "RUST_TOOLCHAIN:     $RUST_TOOLCHAIN"
echo "PYODIDE_ABI_VERSION: $PYODIDE_ABI_VERSION"
echo "PYODIDE_RUSTFLAGS:  $PYODIDE_RUSTFLAGS"
echo "PYODIDE_CFLAGS:     $PYODIDE_CFLAGS"
```

Install the matching Rust toolchain and wasm target:

```bash
rustup toolchain install $RUST_TOOLCHAIN
rustup target add --toolchain $RUST_TOOLCHAIN wasm32-unknown-emscripten
```

Install Emscripten via the Pyodide cross-build environment rather than a
stock emsdk. This pins the Emscripten version matching the target Pyodide ABI
automatically, and applies [Pyodide's patches to
Emscripten](https://github.com/pyodide/pyodide/tree/main/emsdk/patches) —
several of which affect dynamic linking of Rust side modules:

```bash
export PYODIDE_XBUILDENV_PATH="$HOME/.cache/pyodide-xbuildenv"
pyodide_cmd xbuildenv install
pyodide_cmd xbuildenv install-emscripten
source "$PYODIDE_XBUILDENV_PATH/$(pyodide_cmd xbuildenv version)/emsdk/emsdk_env.sh"
```

Build the wheel. Notes on the environment variables:

- `MATURIN_PYEMSCRIPTEN_PLATFORM_VERSION` is required for the wheel to get the
  PyPI-accepted `pyemscripten_*` platform tag instead of the legacy
  `emscripten_x_y_z` tag PyPI rejects (this also needs a recent maturin, hence
  `uvx maturin` rather than the project venv's maturin).
- `CFLAGS_wasm32_unknown_emscripten` is needed for crates that compile C code
  (e.g. zstd-sys in arro3-io): Pyodide's cflags include `-fPIC`, without which
  the C objects can't be linked into a `SIDE_MODULE` (errors like "relocation
  R_WASM_MEMORY_ADDR_SLEB cannot be used ...; recompile with -fPIC").
- Always build with `--release`: debug builds are ~10x larger (full DWARF) and
  slow.

```bash
# arro3-core
RUSTUP_TOOLCHAIN=$RUST_TOOLCHAIN \
CARGO_TARGET_WASM32_UNKNOWN_EMSCRIPTEN_RUSTFLAGS="$PYODIDE_RUSTFLAGS" \
CFLAGS_wasm32_unknown_emscripten="$PYODIDE_CFLAGS" \
MATURIN_PYEMSCRIPTEN_PLATFORM_VERSION=$PYODIDE_ABI_VERSION \
    uvx maturin build \
    --release \
    -o dist \
    -m arro3-core/Cargo.toml \
    --target wasm32-unknown-emscripten \
    -i python$PYTHON_VERSION

# arro3-compute
RUSTUP_TOOLCHAIN=$RUST_TOOLCHAIN \
CARGO_TARGET_WASM32_UNKNOWN_EMSCRIPTEN_RUSTFLAGS="$PYODIDE_RUSTFLAGS" \
CFLAGS_wasm32_unknown_emscripten="$PYODIDE_CFLAGS" \
MATURIN_PYEMSCRIPTEN_PLATFORM_VERSION=$PYODIDE_ABI_VERSION \
    uvx maturin build \
    --release \
    -o dist \
    -m arro3-compute/Cargo.toml \
    --target wasm32-unknown-emscripten \
    -i python$PYTHON_VERSION

# arro3-io
RUSTUP_TOOLCHAIN=$RUST_TOOLCHAIN \
CARGO_TARGET_WASM32_UNKNOWN_EMSCRIPTEN_RUSTFLAGS="$PYODIDE_RUSTFLAGS" \
CFLAGS_wasm32_unknown_emscripten="$PYODIDE_CFLAGS" \
MATURIN_PYEMSCRIPTEN_PLATFORM_VERSION=$PYODIDE_ABI_VERSION \
    uvx maturin build \
    --release \
    -o dist \
    -m arro3-io/Cargo.toml \
    --target wasm32-unknown-emscripten \
    -i python$PYTHON_VERSION \
    --no-default-features
```

`--no-default-features` applies to `arro3-io` only: it disables the `async`
feature, which doesn't compile for emscripten.

Verify the wheel filename ends in `pyemscripten_${PYODIDE_ABI_VERSION}_wasm32.whl` before considering it publishable.

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
