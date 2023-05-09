import json

from RequestHandler import RequestHandler


class EMInfraImporter:
    def __init__(self, request_handler: RequestHandler):
        self.request_handler = request_handler
        self.request_handler.requester.first_part_url += 'eminfra/'

    def import_agents_from_webservice_page_by_page(self, page_size: int, cursor: str = '') -> ([dict], str):
        return self.get_objects_from_oslo_search_endpoint(url_part='agents', size=page_size, cursor=cursor,
                                                          contact_info=True)

    def import_betrokkenerelaties_from_webservice_page_by_page(self, page_size: int, cursor: str = '') -> ([dict], str):
        return self.get_objects_from_oslo_search_endpoint(url_part='betrokkenerelaties', size=page_size, cursor=cursor)

    def import_assetrelaties_from_webservice_page_by_page(self, page_size: int, cursor: str = '') -> ([dict], str):
        return self.get_objects_from_oslo_search_endpoint(url_part='assetrelaties', size=page_size, cursor=cursor)

    def import_assets_from_webservice_page_by_page(self, page_size: int, cursor: str = '') -> ([dict], str):
        return self.get_objects_from_oslo_search_endpoint(url_part='assets', size=page_size, cursor=cursor)

    def get_objects_from_oslo_search_endpoint(self, url_part: str, cursor: str = '',
                                              filter_string: str = '{}', size: int = 100, contact_info: bool = False,
                                              expansions_string: str = '{}') -> ([dict], str):
        url = f'core/api/otl/{url_part}/search'
        if contact_info:
            url += '?expand=contactInfo'
        body_fixed_part = '{"size": ' + f'{size}' + ''
        if filter_string != '{}':
            body_fixed_part += ', "filters": ' + filter_string
        if expansions_string != '{}':
            body_fixed_part += ', "expansions": ' + expansions_string

        json_list = []

        body = body_fixed_part
        if cursor != '':
            body += f', "fromCursor": "{cursor}"'
        body += '}'
        json_data = json.loads(body)

        response = self.request_handler.perform_post_request(url=url, json_data=json_data)

        decoded_string = response.content.decode("utf-8")
        dict_obj = json.loads(decoded_string)
        json_list.extend(dict_obj['@graph'])
        if 'em-paging-next-cursor' not in response.headers:
            cursor = ''
        else:
            cursor = response.headers['em-paging-next-cursor']

        return json_list, cursor
