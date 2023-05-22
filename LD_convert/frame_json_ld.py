import json
import logging
import time
from pathlib import Path

import pyld
from rdflib import Graph


def convert_jsonld_to_ttl(file: Path):
    logging.info(f'Starting conversion of {file.name}')
    start = time.time()
    g = Graph()
    g.parse(str(file), format='json-ld')

    namespace_dict = {}
    for ns_prefix, namespace in g.namespaces():
        namespace_dict[ns_prefix] = namespace

    q_res = g.query("SELECT ?o WHERE { ?s a ?o }")
    type_lijst = []
    for row in q_res:
        if not str(row.o).startswith('http://www.opengis.net/ont/sf'):
            type_lijst.append(str(row.o))

    json_ld = g.serialize(format='json-ld', indent=4)
    json_ld = json.loads(json_ld)
    compacted = pyld.jsonld.compact(json_ld, {}, options={'graph': False})
    compacted = pyld.jsonld.frame(compacted, {
        '@context': namespace_dict,
        '@type': type_lijst})

    file_path = Path(str(file).replace('.jsonld', '_2.jsonld'))

    with open(str(file_path), "w") as out_file:
        json.dump(obj=compacted, fp=out_file, indent=4)
    end = time.time()
    logging.info(f'Converted {file.name} to ttl in {round(end - start, 2)} seconds')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    this_directory = Path(__file__).parent
    convert_jsonld_to_ttl(this_directory / 'response.jsonld')
