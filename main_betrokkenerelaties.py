from pathlib import Path

from EMInfraImporter import EMInfraImporter
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager
from SyncManager import SyncManager

if __name__ == '__main__':
    environment = 'prd'

    settings_manager = SettingsManager(
        settings_path='/home/davidlinux/Documents/AWV/resources/settings_AwvinfraPostGISSyncer.json')
    requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type='JWT', env=environment,
                                                  multiprocessing_safe=True)
    request_handler = RequestHandler(requester)
    eminfra_importer = EMInfraImporter(request_handler)

    this_directory = Path(__file__).parent
    shelve_path = Path(this_directory / 'shelve')

    otl_db_path = Path(this_directory / 'OTL 2.7.db')

    sync_manager = SyncManager(shelve_path=shelve_path, otl_db_path=otl_db_path, eminfra_importer=eminfra_importer,
                               resource_main_dir=Path('/home/davidlinux/Documents/AWV/jsonld/20230508'))
    #sync_manager.download_resource('betrokkenerelaties')
    sync_manager.combine_jsons('betrokkenerelaties', combine_max=500)
