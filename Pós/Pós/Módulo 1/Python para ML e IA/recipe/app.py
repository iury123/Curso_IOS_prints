from flask import Flask, jsonify, request
from database import db
from auth import jwt
import models
from controllers import user_bp, recipe_bp
from swagger import swagger

app = Flask(__name__)
app.config.from_object('config')
app.register_blueprint(user_bp)
app.register_blueprint(recipe_bp, url_prefix='/recipes')

db.init_app(app)
jwt.init_app(app)
swagger.init_app(app)

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        print("Database tables created.")
    app.run(debug=True)    
