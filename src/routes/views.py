from flask import Blueprint, jsonify, request

app_routes = Blueprint("app_routes", __name__)


@app_routes.route("/api/scraped-data/<int:id>", methods=["GET"])
def fetch_data_by_id(id):
    return jsonify({"message": f"Fetching data for ID: {id}"})


@app_routes.route("/api/scraped-data", methods=["GET"])
def fetch_all_data():
    return jsonify({"message": "Fetching all data"})


@app_routes.route("/api/scraped-data", methods=["POST"])
def save_data():
    data = request.json
    return jsonify({"message": "Data saved", "data": data})


@app_routes.route("/api/scraped-data/batch", methods=["POST"])
def save_multiple_data():
    data = request.json
    return jsonify({"message": "Multiple data entries saved", "data": data})
