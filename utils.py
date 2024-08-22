
def process_string(x):
    return add_quotes(x.replace("\'", "\'\'")) if x else 'NULL'

def add_quotes(x):
    return "\'{}\'".format(x)