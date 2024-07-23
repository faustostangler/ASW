import re

# system wide
batch_size = 50
database_folder = 'backend/data'

# selenium 
driver_wait_time = 2 # 5
driver = wait = None

# b3 info
words_to_remove = [
    '  EM LIQUIDACAO', ' EM LIQUIDACAO', ' EXTRAJUDICIAL', 
    '  EM RECUPERACAO JUDICIAL', '  EM REC JUDICIAL', 
    ' EM RECUPERACAO JUDICIAL', ' EM LIQUIDACAO EXTRAJUDICIAL'
]
words_to_remove = '|'.join(map(re.escape, words_to_remove))
