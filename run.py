"""
Application Entry Point & WSGI Runner.
Executes the Flask application in development or serves as Gunicorn entry point in production.
"""

import os
from app import create_app

# Instantiate Flask application using Application Factory
app = create_app()

if __name__ == "__main__":
    host = app.config.get("HOST", "127.0.0.1")
    port = app.config.get("PORT", 5000)
    debug = app.config.get("DEBUG", False)
    
    app.logger.info(f"Starting server on http://{host}:{port} (Debug: {debug})")
    app.run(host=host, port=port, debug=debug)
