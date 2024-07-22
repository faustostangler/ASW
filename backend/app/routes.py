from app import app
from flask import jsonify, request
import pandas as pd

@app.route('/')
def index():
    return "Welcome to AssetWise!"

@app.route('/api/data', methods=['GET'])
def get_data():
    # Exemplo: Carregar dados de um arquivo CSV usando Pandas
    df = pd.read_csv('data/sample.csv')
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/data', methods=['POST'])
def add_data():
    # Exemplo: Adicionar dados ao banco de dados
    data = request.get_json()
    # Adicione sua lógica de processamento e salvamento de dados aqui
    return jsonify({"message": "Data added successfully"}), 201
