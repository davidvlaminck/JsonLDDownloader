import logging
import time
from multiprocessing import Pool
from pathlib import Path

from rdflib import Dataset


def convert_jsonld_to_trig(file: Path):
    logging.info(f'Starting conversion of {file.name}')
    start = time.time()
    g = Dataset()
    g.parse(str(file), format='json-ld')
    g.serialize(str(file).replace('.jsonld', '.trig').replace('20230522', '20230522/combined'), format='trig')
    end = time.time()
    logging.info(f'Converted {file.name} to trig in {round(end - start, 2)} seconds')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    done = []
    files_to_do = [file for file in Path('/home/davidlinux/Documents/AWV/jsonld/20230522/assets').iterdir() if
                   file.name.endswith('.jsonld') and file.name not in done]

    with Pool(1) as p:
        result = p.map(convert_jsonld_to_trig, files_to_do)
