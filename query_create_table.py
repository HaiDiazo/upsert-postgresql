import json

type_data = {
    'long': 'int8',
    'keyword': 'text',
    'text': 'text',
    'date': 'timestamp',
    'float': 'double precision',
    'boolean': 'boolean'
}

if __name__ == "__main__":
    with open('smm_raw.json', 'r') as file:
        datas: dict = json.load(file)

    results = []
    for key, value in datas['properties'].items():
        if 'type' in value:
            results.append(f"{key} {type_data.get(value['type'])}")

        if 'properties' in value:
            results.append(f"{key} json")
    query = f"CREATE TABLE tb_posts ({', '.join(result for result in results)});"
    print(query)


