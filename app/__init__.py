"""
Salesforce Bulk API 2.0 Ingestion Application Factory.
Initializes Flask app instance, loads configuration, configures enterprise logging,
connects database layer, and registers middleware and API blueprints.
"""

from typing import Optional
from flask import Flask
from flask_marshmallow import Marshmallow
from app.config import get_config
from app.database.database import init_db
from app.middleware.logging import setup_logging, register_request_logging
from app.middleware.error_handler import register_error_handlers
from app.api.routes import api_bp

# Initialize Marshmallow extension singleton
ma = Marshmallow()

def create_app(config_name: Optional[str] = None) -> Flask:
    """
    Application Factory Pattern.
    
    Args:
        config_name: Environment name ('development', 'testing', 'production')
                     or None to read from FLASK_ENV.
                     
    Returns:
        Flask: Fully configured Flask application instance.
    """
    app = Flask(__name__)

    # 1. Load Configuration
    config = get_config(config_name)
    app.config.from_object(config)

    # 2. Setup Enterprise Logging
    setup_logging(app)

    # 3. Initialize Extensions (SQLAlchemy & Marshmallow)
    init_db(app)
    ma.init_app(app)

    # 4. Register Middlewares (Request Logging & Global Error Handlers)
    register_request_logging(app)
    register_error_handlers(app)

    # 5. Register API Blueprints
    app.register_blueprint(api_bp)

    app.logger.info(
        f"Application initialized successfully in '{app.config.get('ENVIRONMENT')}' mode."
    )
    return app
