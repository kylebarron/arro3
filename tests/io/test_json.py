from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
from arro3.io import infer_json_schema, read_json, write_ndjson


def test_json_roundtrip():
    table = pa.table({"a": [1, 2, 3, 4]})
    # We can't use tmp_path fixture with pytest-freethreading
    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)
        write_ndjson(table, tmp_path / "test.json")

        schema = infer_json_schema(tmp_path / "test.json")

        table_retour = pa.table(read_json(tmp_path / "test.json", schema))
        assert table == table_retour
