
def process_string(x):
    return "\'{}\'".format(x.replace("\'", "\'\'")) if x else 'NULL' 