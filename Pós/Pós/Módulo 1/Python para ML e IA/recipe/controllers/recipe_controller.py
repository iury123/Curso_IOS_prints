from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required
from models import Recipe
from database import db

recipe_bp = Blueprint('recipe', __name__)

@recipe_bp.route('', methods=['POST'])
@jwt_required()
def create_recipe():
    """
    Cria uma nova receita.
    ---
    security:
      - BearerAuth: []
    parameters:
      - in: body
        name: body
        schema:
          type: object
          required: true
          properties:
            title:
              type: string
            ingredients:
              type: string
            time_minutes:
              type: integer
    responses:
      201:
        description: Receita criada com sucesso
      401:
        description: Token não fornecido ou inválido
    """
    data = request.get_json()
    new_recipe = Recipe(
        title=data['title'],
        ingredients=data['ingredients'],
        time_minutes=data['time_minutes']
    )
    db.session.add(new_recipe)
    db.session.commit()
    return jsonify({"msg": "Recipe created"}), 201

@recipe_bp.route('', methods=['GET'])
def get_recipes():
    """
    Lista receitas com filtros opcionais.

    ---
    parameters:
      - in: query
        name: ingredient
        type: string
        required: false
        description: Filtra por ingrediente
      - in: query
        name: max_time
        type: integer
        required: false
        description: Tempo máximo de preparo (minutos)
    responses:
      200:
        description: Lista de receitas filtradas
        schema:
          type: array
          items:
            type: object
            properties:
              id:
                type: integer
              title:
                type: string
              ingredients:
                type: string
              time_minutes:
                type: integer
    """
    ingredient = request.args.get('ingredient')
    max_time = request.args.get('max_time', type=int)

    query = Recipe.query

    if ingredient:
        query = query.filter(Recipe.ingredients.like(f'%{ingredient}%'))
    
    if max_time is not None:
        query = query.filter(Recipe.time_minutes <= max_time)

    recipes = query.all()
    return jsonify([{
        "id": r.id,
        "title": r.title,
        "ingredients": r.ingredients,
        "time_minutes": r.time_minutes
    } for r in recipes]), 200


@recipe_bp.route('/<int:recipe_id>', methods=['PUT'])
@jwt_required()
def update_recipe(recipe_id):
    """
    Atualiza uma receita existente.
    ---
    security:
      - BearerAuth: []
    parameters:
      - in: path
        name: recipe_id
        required: true
        type: integer
      - in: body
        name: body
        schema:
          type: object
          properties:
            title:
              type: string
            ingredients:
              type: string
            time_minutes:
              type: integer
    responses:
      200:
        description: Receita atualizada
      404:
        description: Receita não encontrada
      401:
        description: Token não fornecido ou inválido
    """
    data = request.get_json()
    recipe = Recipe.query.get_or_404(recipe_id)
    
    if 'title' in data:
        recipe.title = data['title']
    if 'ingredients' in data:
        recipe.ingredients = data['ingredients']
    if 'time_minutes' in data:
        recipe.time_minutes = data['time_minutes']
        
    db.session.commit()
    return jsonify({"msg": "Recipe updated"}), 200