from flask import Flask
from flask_cors import CORS
from results.controller import results_bp  # your blueprint

# Create the Flask app object directly
app = Flask(__name__)
CORS(app)

# Register your blueprint
app.register_blueprint(results_bp)

# Simple test route
@app.route("/")
def index():
    return "<h1>Backend OK</h1><p>Use <a href='/result'>/result</a></p>"

# Only run this if running locally
if __name__ == "__main__":
    app.run(debug=True, port=5000)
