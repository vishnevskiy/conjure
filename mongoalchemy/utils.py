import pymongo

def transform_keys(keys):
    transformed_keys = []

    for key in keys.split():
       direction = pymongo.ASCENDING

       if key[0] == '-':
           direction = pymongo.DESCENDING
       if key[0] in ('-', '+'):
           key = key[1:]

       transformed_keys.append((key, direction))

    return transformed_keys