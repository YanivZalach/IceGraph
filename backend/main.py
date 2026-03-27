from dotenv import load_dotenv
from flask import (
    Flask,
    jsonify,
    request,
    Response,
    redirect,
    send_from_directory,
)
from pyspark.errors import AnalysisException

from constants import APPLICATION_PORT
from iceberg_inventory_builder import IcebergInventoryBuilder
from icegraph_logger import logger
from icegraph_data_normalizer import normalize_graph_data
from utils import verify_iceberg_table

load_dotenv()
app = Flask(__name__, static_url_path="/static")


@app.route("/", methods=["GET"])
def react_app():
    return send_from_directory("static/react", "index.html")


@app.route("/api/v1/graph-data", methods=["POST"])
def graph_data():
    table_name = request.form.get("table_name")
    date_value = request.form.get("date")

    try:
        verify_iceberg_table(table_name)
        table_data = IcebergInventoryBuilder(table_name, date_value).collect()
        data = normalize_graph_data(table_data)

        return jsonify(data)

    except AnalysisException as e:
        logger.error(f"Spark Error: {e}")
        return jsonify({"error": str(e)}), 400


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=APPLICATION_PORT)
