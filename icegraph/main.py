import os

from dotenv import load_dotenv
from flask import (
    Flask,
    request,
    render_template,
    Response,
    redirect,
    send_from_directory,
)
from pyspark.errors import AnalysisException

from iceberg_inventory_builder import IcebergInventoryBuilder
from icegraph_visualizer import IceGraphVisualizer
from spark_connect import open_spark_connect_session
from utils import verify_iceberg_table

load_dotenv()
app = Flask(__name__, static_url_path="/static")
spark = open_spark_connect_session()


@app.route("/lib/<path:path>")
def send_lib(path):
    return send_from_directory("lib", path)


@app.route("/", methods=["GET"])
def home():
    error_flag = request.args.get("error")

    return render_template("index.html", error=error_flag)


@app.route("/generate", methods=["POST"])
def generate():
    table_name = request.form.get("table_name")
    date_value = request.form.get("date")

    try:
        verify_iceberg_table(table_name)
        table_data = IcebergInventoryBuilder(table_name, date_value).collect()
        html = IceGraphVisualizer(table_data).generate()

        return Response(html, mimetype="text/html")

    except AnalysisException as e:
        print(f"Spark Error: {e}")
        return redirect("/?error=table_not_found")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ["APPLICATION_PORT"]))
