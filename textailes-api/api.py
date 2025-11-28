from flask import Flask, send_from_directory
from flask_restful import Api
from flasgger import Swagger

from services.storage import init_minio_bucket

# --- Import resources from all subprojects ---
from resources.artifact import ArtifactResource, ArtifactItemResource
from resources.sensor import SensorReadingResource

# Temporary placement, will move later
init_minio_bucket()

app = Flask(__name__)

# Swagger Setup
@app.route('/swagger.json')
def swagger_spec():
    return send_from_directory('static', 'swagger.json')

Swagger(app, config={
    'specs': [
        {'endpoint': 'swagger', 'route': '/swagger.json',
         'rule_filter': lambda rule: True, 'model_filter': lambda tag: True}
    ],
    'static_url_path': '/flasgger_static',
    'swagger_ui': True,
    'specs_route': '/docs',
    'headers': []
})

api = Api(app)

# ---Register Routes for all subprojects ---
api.add_resource(ArtifactResource, '/artifacts')
api.add_resource(ArtifactItemResource, '/artifacts/<string:artifact_id>')
api.add_resource(SensorReadingResource, '/sensor-readings')

@app.route('/health')
def health_check():
    """Return API health status with current timestamp."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now(timezone.utc).isoformat()
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)