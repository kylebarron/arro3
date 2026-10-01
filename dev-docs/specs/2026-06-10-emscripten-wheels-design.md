# Emscripten (PEP 783) wheels for arro3-core and arro3-compute

**Date:** 2026-06-10
**Status:** Approved design. Stage 0 spike complete (compilation + platform tag verified).

## Goal

Publish Pyodide-compatible Emscripten wheels for `arro3-core` and `arro3-compute` to PyPI as part of the normal release pipeline, replacing the old self-hosted GitHub-Releases distribution model in `pyodide-wheels.yml`.

`arro3-io` is explicitly out of scope (its `object_store`/`tokio` dependency tree is an unknown under emscripten and not needed now).

## Background

- [PEP 783](https://peps.python.org/pep-0783/) (accepted) defines the PyEmscripten platform. PyPI now accepts wheels tagged `pyemscripten_2025_0` (Python 3.13 / Pyodide 0.29.x) and `pyemscripten_2026_0` (Python 3.14 / Pyodide 314.x).
- Wheels install via pip/micropip from PyPI like any other platform wheel — no separate hosting or `pyodide-lock.json` entry needed.
- [pydantic-core's writeup](https://pydantic.dev/articles/emscripten-wheels-pydantic) provides a verified maturin-action-based workflow; this design follows it closely.

## Targets (verified locally 2026-06-10 via `uvx -p X --from pyodide-build pyodide config get`)

| Python | Platform tag          | Emscripten | Rust toolchain       |
|--------|-----------------------|------------|----------------------|
| 3.13   | `pyemscripten_2025_0` | 4.0.9      | `nightly-2025-02-01` |
| 3.14   | `pyemscripten_2026_0` | 5.0.3      | `1.93.0` (stable)    |

One wheel per (Python version × module): 4 wheels total per release. abi3 does not apply to emscripten. All toolchain values are queried from `pyodide config get` at build time, never hardcoded; `pyodide config get` answers depend on the Python interpreter pyodide-build runs under.

## Stage 0: Local compile spike

**Status:** Compilation validated (both modules build to `wasm32-unknown-emscripten`; see `DEVELOP.md` "Emscripten Python wheels" for the working instructions). Two findings from the first run:

1. **Wrong platform tag:** maturin 1.9.4 emitted `emscripten_4_0_9_wasm32`, which PyPI rejects. Fix: recent maturin + `MATURIN_PYEMSCRIPTEN_PLATFORM_VERSION=<abi>` → `pyemscripten_*` tag. DEVELOP.md updated accordingly.
2. **Toolchain/interpreter mismatch:** the first cp314 wheel was built with the 3.13-line config (emscripten 4.0.9). Correct 3.14 builds use emscripten 5.0.3 + Rust 1.93.0, resolved by querying config under the matching interpreter (`uvx -p`).

**Tag verification (done 2026-06-10):** rebuilding with the updated DEVELOP.md instructions (uvx maturin 1.13.3 + `MATURIN_PYEMSCRIPTEN_PLATFORM_VERSION`) produced `arro3_core-0.8.0-cp314-cp314-pyemscripten_2026_0_wasm32.whl` — the PyPI-accepted tag. Stage 0 is complete.

## Stage 1: `emscripten` job in `wheels.yml`

**Goal:** Emscripten wheels build on release tags and flow into the existing trusted-publishing release job.
**Success criteria:** A `py-v*` tag run publishes 4 emscripten wheels to PyPI alongside the native wheels.
**Status:** Implemented (2026-06-10); pending a `workflow_dispatch` CI run to verify end-to-end. pyodide-build moved from dev deps to a dedicated `pyodide` dependency group (with `python_version >= '3.12'` marker) so CI installs only that group via `uv sync --only-group pyodide`.

New job, matching the file's existing style:

```yaml
emscripten:
  runs-on: ubuntu-latest
  strategy:
    matrix:
      python-version: ["3.13", "3.14"]
      module:
        - arro3-core
        - arro3-compute
  steps:
    - uses: actions/checkout@v4

    - name: Install uv
      uses: astral-sh/setup-uv@v7
      with:
        python-version: ${{ matrix.python-version }}

    # pyodide-build is a locked dev dependency (uv.lock is the version pin)
    - name: Install dev environment
      run: uv sync

    - name: Get pyodide config
      id: pyodide-config
      run: |
        echo "rust-toolchain=$(uv run pyodide config get rust_toolchain)" >> "$GITHUB_OUTPUT"
        echo "emscripten-version=$(uv run pyodide config get emscripten_version)" >> "$GITHUB_OUTPUT"
        echo "pyodide-abi-version=$(uv run pyodide config get pyodide_abi_version)" >> "$GITHUB_OUTPUT"
        echo "rustflags=$(uv run pyodide config get rustflags)" >> "$GITHUB_OUTPUT"

    - uses: mymindstorm/setup-emsdk@v14
      with:
        version: ${{ steps.pyodide-config.outputs.emscripten-version }}

    - name: Build wheels
      uses: PyO3/maturin-action@v1
      env:
        CARGO_TARGET_WASM32_UNKNOWN_EMSCRIPTEN_RUSTFLAGS: ${{ steps.pyodide-config.outputs.rustflags }}
        MATURIN_PYEMSCRIPTEN_PLATFORM_VERSION: ${{ steps.pyodide-config.outputs.pyodide-abi-version }}
      with:
        target: wasm32-unknown-emscripten
        rust-toolchain: ${{ steps.pyodide-config.outputs.rust-toolchain }}
        args: --release --out dist -i ${{ matrix.python-version }} --manifest-path ${{ matrix.module }}/Cargo.toml

    - name: Upload wheels
      uses: actions/upload-artifact@v4
      with:
        name: wheels-emscripten-py${{ matrix.python-version }}-${{ matrix.module }}
        path: dist
```

Key decisions:

- **No hardcoded toolchain pins.** Rust toolchain, emscripten version, rustflags, and ABI tag all come from `pyodide config get`. The single source of truth is the `pyodide-build` version locked in `uv.lock`; bumping that lock entry is the only maintenance event.
- **`MATURIN_PYEMSCRIPTEN_PLATFORM_VERSION`** is required to emit the PyPI-accepted `pyemscripten_*` tag (verified: without it, maturin 1.9.4 emits a rejected legacy tag). maturin-action installs a current maturin by default; if CI still emits a legacy tag, pin `maturin-version` explicitly in the action.
- **CI checks the tag:** add a step asserting the built wheel filename contains `pyemscripten_` before upload, so a silent maturin regression can't publish a rejected/legacy-tagged wheel.
- **Release integration:** add `emscripten` to the release job's `needs:` list. The artifact name `wheels-emscripten-py3.13-arro3-core` already matches the existing download pattern `wheels-*-${{ matrix.module }}`, so no publish-step changes are needed.
- **Blocking:** the emscripten job is a hard release requirement, same as the native platforms. Acceptable because builds are deterministic given the locked pyodide-build, and module support is proven by Stage 0.

## Stage 2: Remove `pyodide-wheels.yml`

**Goal:** Delete the superseded workflow (manual emsdk install, GitHub-Releases hosting, floating `@nightly` toolchain).
**Success criteria:** File removed; no other references to the `pyodide-v*` release tags remain in docs/CI.
**Status:** Complete (2026-06-10). Remaining references are historical CHANGELOG entries only.

## Testing

- Stage 0 proves compilation per module/Python version (done) and the platform tag (pending one rebuild).
- A `workflow_dispatch` run of `wheels.yml` proves the CI job end-to-end before any release tag.
- An in-Pyodide import smoke test (`pyodide venv` + node) is deliberately deferred — not needed for v1.

## Out of scope

- `arro3-io` emscripten support
- Python 3.12 / Pyodide 0.27 (older line; add later if users ask)
- Runtime testing inside a Pyodide environment
