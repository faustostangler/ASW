import re

# system wide
batch_size = 50
database_folder = 'backend/data'

# selenium 
wait_time = 2 # 5
driver = driver_wait = None

# b3 info
companies_url = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/search?language=pt-br"

judicial = [
    '  EM LIQUIDACAO', ' EM LIQUIDACAO', ' EXTRAJUDICIAL', 
    '  EM RECUPERACAO JUDICIAL', '  EM REC JUDICIAL', 
    ' EM RECUPERACAO JUDICIAL', ' EM LIQUIDACAO EXTRAJUDICIAL'
]
words_to_remove = '|'.join(map(re.escape, judicial))
governance_levels = {
    "NM": "Cia. Novo Mercado",
    "N1": "Cia. Nível 1 de Governança Corporativa",
    "N2": "Cia. Nível 2 de Governança Corporativa",
    "MA": "Cia. Bovespa Mais",
    "M2": "Cia. Bovespa Mais Nível 2",
    "MB": "Cia. Balcão Org. Tradicional",
    "DR1": "BDR Nível 1",
    "DR2": "BDR Nível 2",
    "DR3": "BDR Nível 3",
    "DRE": "BDR de ETF",
    "DRN": "BDR Não Patrocinado"
}
