from app import db

class FinancialData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    field1 = db.Column(db.String(64))
    field2 = db.Column(db.String(64))
    # Adicione outros campos conforme necessário
