import threading
import traceback
from dotenv import load_dotenv
from pathlib import Path
from flask import (
    Flask,
    jsonify,
    request,
    send_from_directory,
)
from pyspark.errors import AnalysisException

from constants import APPLICATION_PORT
from iceberg_inventory_builder import IcebergInventoryBuilder
from iceberg_metadata_snapshot_map import collect_snapshot_map
from icegraph_logger import logger
from icegraph_data_normalizer import normalize_graph_data
from utils import verify_iceberg_table

load_dotenv()
app = Flask(__name__, static_url_path="/static")

_in_flight_lock = threading.Lock()
_in_flight: dict = {}


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve(path):
    static_path = Path(app.static_folder).resolve()
    requested_path = (static_path / path).resolve()

    if requested_path.is_file():
        return send_from_directory(static_path, path)

    return send_from_directory(static_path, "index.html")


@app.route("/api/v1/snapshot-map/<path:table_name>", methods=["GET"])
def snapshot_map(table_name):
    try:
        verify_iceberg_table(table_name)

        result = collect_snapshot_map(table_name)

        return jsonify(result)

    except AnalysisException as e:
        logger.error(f"Spark Error: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 400

    except Exception as e:
        logger.error(f"Unexpected error: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/graph-data", methods=["POST"])
def graph_data():
    table_name = request.form.get("table_name")
    start_snapshot_id = request.form.get("start_snapshot_id")
    if start_snapshot_id:
        start_snapshot_id = int(start_snapshot_id)
    end_snapshot_id = request.form.get("end_snapshot_id")
    if end_snapshot_id:
        end_snapshot_id = int(end_snapshot_id)
    key = (table_name, start_snapshot_id, end_snapshot_id)

    with _in_flight_lock:
        if key in _in_flight:
            state = _in_flight[key]
            is_leader = False
        else:
            state = {"event": threading.Event(), "result": None, "error": None}
            _in_flight[key] = state
            is_leader = True

    if not is_leader:
        logger.info(f"Duplicate request for {key}, waiting for in-flight result")
        state["event"].wait()
        if state["error"]:
            return jsonify({"error": state["error"]}), 400
        return jsonify(state["result"])

    try:
        verify_iceberg_table(table_name)
        table_data = IcebergInventoryBuilder(*key).collect()
        state["result"] = normalize_graph_data(table_data)
        return jsonify(state["result"])

    except AnalysisException as e:
        logger.error(f"Spark Error: {e}\n{traceback.format_exc()}")
        state["error"] = str(e)
        return jsonify({"error": str(e)}), 400

    except Exception as e:
        logger.error(f"Unexpected error: {e}\n{traceback.format_exc()}")
        state["error"] = str(e)
        return jsonify({"error": str(e)}), 500

    finally:
        state["event"].set()
        with _in_flight_lock:
            _in_flight.pop(key, None)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=APPLICATION_PORT)
