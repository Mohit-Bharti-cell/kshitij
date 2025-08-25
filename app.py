from flask import Flask
from flask_cors import CORS
from results.controller import results_bp

def create_app(**kwargs):   # <-- this fixes the gunicorn error
    app = Flask(__name__)
    CORS(app)

    # register your blueprint
    app.register_blueprint(results_bp)

    @app.route("/")
    def index():
        return "<h1>Backend OK</h1><p>Use <a href='/result'>/result</a></p>"

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True, port=5000)
