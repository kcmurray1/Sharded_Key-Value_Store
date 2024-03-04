from flask import Flask

def create_app():
    app = Flask(__name__)

    from .views_route import views_route
    app.register_blueprint(views_route)
    
    return app
