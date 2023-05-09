import json
import shelve
from pathlib import Path

from EMInfraImporter import EMInfraImporter
from JsonLdCompleter import JsonLdCompleter


class SyncManager:
    def __init__(self, shelve_path: Path, otl_db_path: Path, eminfra_importer: EMInfraImporter,
                 resource_main_dir: Path):
        self.shelve_path: Path = shelve_path
        self.db: dict = {}
        if not Path.is_file(self.shelve_path):
            with shelve.open(str(self.shelve_path)):
                pass
            self._save_to_shelf(entries={})
        self.eminfra_importer = eminfra_importer
        self.jsonld_completer = JsonLdCompleter(otl_db_path)
        self.resource_main_dir = resource_main_dir

    def _show_shelve(self) -> None:
        for key in self.db.keys():
            print(f'{key}: {self.db[key]}')

    def _save_to_shelf(self, entries: dict) -> None:
        with shelve.open(str(self.shelve_path), writeback=True) as db:
            for k, v in entries.items():
                db[k] = v

            self.db = dict(db)

    def download_resource(self, resource_name: str):
        if not Path.is_dir(self.resource_main_dir / resource_name):
            Path.mkdir(self.resource_main_dir / Path(resource_name))

        if resource_name not in self.db:
            self._save_to_shelf({resource_name: {'cursor': '', 'page_size': 100, 'done': False, 'page': 0}})

        while not self.db[resource_name]['done']:
            objects, cursor = None, None
            if resource_name == 'assets':
                objects, cursor = self.eminfra_importer.import_assets_from_webservice_page_by_page(
                    page_size=self.db[resource_name]['page_size'], cursor=self.db[resource_name]['cursor'])
            elif resource_name == 'assetrelaties':
                objects, cursor = self.eminfra_importer.import_assetrelaties_from_webservice_page_by_page(
                    page_size=self.db[resource_name]['page_size'], cursor=self.db[resource_name]['cursor'])
            elif resource_name == 'agents':
                objects, cursor = self.eminfra_importer.import_agents_from_webservice_page_by_page(
                    page_size=self.db[resource_name]['page_size'], cursor=self.db[resource_name]['cursor'])
            elif resource_name == 'betrokkenerelaties':
                objects, cursor = self.eminfra_importer.import_betrokkenerelaties_from_webservice_page_by_page(
                    page_size=self.db[resource_name]['page_size'], cursor=self.db[resource_name]['cursor'])

            self.process_objects(resource_name=resource_name, objects=objects, index=self.db[resource_name]['page'])

            self._save_to_shelf({resource_name: {'cursor': cursor, 'page_size': self.db[resource_name]['page_size'],
                                                 'done': (cursor == ''), 'page': self.db[resource_name]['page'] + 1}})

    def process_objects(self, resource_name, objects, index):
        assets_ld_string = self.jsonld_completer.transform_json_ld(objects)
        this_directory = self.resource_main_dir
        file_path = Path(this_directory / f'{resource_name}/{resource_name}_{index}.jsonld')
        with open(file_path, 'w') as f:
            f.write(assets_ld_string)
        pass

    def combine_jsons(self, resource_name: str, combine_max: int = -1):
        # reads all files from assets folder and combines them into one file
        resource_directory = self.resource_main_dir / resource_name

        all_files = [x for x in resource_directory.iterdir() if x.is_file()]
        first_file = all_files[0]
        with open(first_file, 'r') as f:
            first_file = f.read()
        first_file = json.loads(first_file)

        for index, other_file in enumerate(all_files[1:]):
            if combine_max != -1 and index % combine_max == 0 and index != 0:
                with open(Path(resource_directory / f'combined_{index}.jsonld'), 'w') as f:
                    f.write(json.dumps(first_file))

                with open(other_file, 'r') as f:
                    first_file = f.read()
                first_file = json.loads(first_file)
                continue

            with open(other_file, 'r') as f:
                file2 = f.read()
            file2 = json.loads(file2)
            first_file['@graph'].extend(file2['@graph'])

        with open(Path(resource_directory / f'combined_last.jsonld'), 'w') as f:
            f.write(json.dumps(first_file))
