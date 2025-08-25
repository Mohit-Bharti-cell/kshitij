from flask import Blueprint, jsonify
from services.results_service import fetch_results

results_bp = Blueprint("results", __name__)

@results_bp.route("/result", methods=["GET"])
def get_result():
    data = fetch_results()
    return jsonify(data)
