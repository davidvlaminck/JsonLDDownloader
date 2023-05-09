import json
import sqlite3
from pathlib import Path


class JsonLdCompleter:
    def __init__(self, otl_db_path: Path):
        self.valid_uris = self.get_otl_uris_from_db(otl_db_path)

    def transform_json_ld(self, asset_dict):
        new_list = [self.add_exact_geometry(self.fix_dict(asset)) for asset in asset_dict]
        graph_dict = {'@graph': new_list, '@context': {
                'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                'asset': 'https://data.awvvlaanderen.be/id/asset/',
                'lgc': 'https://wegenenverkeer.data.vlaanderen.be/oef/legacy/',
                'ins': 'https://wegenenverkeer.data.vlaanderen.be/oef/inspectie/',
                'ond': 'https://wegenenverkeer.data.vlaanderen.be/oef/onderhoud/',
                'loc': 'https://wegenenverkeer.data.vlaanderen.be/oef/locatie/',
                'tz': 'https://wegenenverkeer.data.vlaanderen.be/oef/toezicht/',
                'geo': 'https://wegenenverkeer.data.vlaanderen.be/oef/geometrie/',
                'grp': 'https://wegenenverkeer.data.vlaanderen.be/oef/groepering/',
                'bz': 'https://wegenenverkeer.data.vlaanderen.be/oef/bezoekfiche/',
                'imel': 'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#',
                'app': 'http://example.org/ApplicationSchema#',
                'abs': 'https://wegenenverkeer.data.vlaanderen.be/ns/abstracten#',
                'geosparql': 'http://www.opengis.net/ont/geosparql#',
                'onderdeel': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#',
                'installatie': 'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#'
            }}
        return json.dumps(graph_dict)

    @staticmethod
    def transform_if_http_value(value):
        if isinstance(value, str) and value.startswith('http'):
            return {"@id": value}
        return value

    def fix_dict(self, old_dict):
        new_dict = {}
        for k, v in old_dict.items():
            if isinstance(v, dict):
                v = self.fix_dict(v)
            elif isinstance(v, list):
                new_list = []
                for item in v:
                    if isinstance(item, dict):
                        new_list.append(self.fix_dict(item))
                    else:
                        new_list.append(self.transform_if_http_value(item))
                v = new_list

            if ':' in k:
                new_dict[k] = self.transform_if_http_value(v)
            elif k in ['RelatieObject.bron', 'RelatieObject.doel']:
                new_dict[
                    'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#' + k] = self.transform_if_http_value(
                    v)
            elif k not in ['@type', '@id']:
                new_dict[self.valid_uris[k]] = self.transform_if_http_value(v)
            else:
                new_dict[k] = v
        return new_dict

    @staticmethod
    def get_otl_uris_from_db(otl_db_path):
        con = sqlite3.connect(otl_db_path)
        cur = con.cursor()
        d = {}
        res = cur.execute("""
SELECT uri FROM OSLOAttributen 
UNION SELECT uri FROM OSLODatatypeComplexAttributen 
UNION SELECT uri FROM OSLODatatypePrimitiveAttributen 
UNION SELECT uri FROM OSLODatatypeUnionAttributen""")
        for row in res.fetchall():
            uri = row[0]
            if '#' in uri:
                short_uri = uri[uri.find('#') + 1:]
            else:
                short_uri = uri[uri.rfind('/') + 1:]
            d[short_uri] = uri

        con.close()
        return d

    @staticmethod
    def add_exact_geometry(new_dict):
        if 'geo:Geometrie.log' in new_dict:
            return new_dict

        if 'loc:Locatie.puntlocatie' in new_dict and 'loc:3Dpunt.puntgeometrie' in new_dict['loc:Locatie.puntlocatie']:
            if 'loc:DtcCoord.lambert72' not in new_dict['loc:Locatie.puntlocatie']['loc:3Dpunt.puntgeometrie']:
                return new_dict
            coords = new_dict['loc:Locatie.puntlocatie']['loc:3Dpunt.puntgeometrie']['loc:DtcCoord.lambert72']
            x = coords['loc:DtcCoordLambert72.xcoordinaat']
            y = coords['loc:DtcCoordLambert72.ycoordinaat']
            z = coords['loc:DtcCoordLambert72.zcoordinaat']

            if x == '' or x is None or x == 0 or x == 0:
                raise ValueError(new_dict)

            new_dict['http://example.org/ApplicationSchema#hasExactGeometry'] = {
                "@id" : new_dict['@id'] + '_geometry',
                "@type": "http://www.opengis.net/ont/sf#Point",
                "http://www.opengis.net/ont/geosparql#asWKT": {
                        "@value": f"<http://www.opengis.net/def/crs/EPSG/9.9.1/31370> Point({x} {y})",
                        "@type": "http://www.opengis.net/ont/geosparql#wktLiteral"
                    }}
        else:
            return new_dict
            new_dict['http://example.org/ApplicationSchema#hasExactGeometry'] = {
                "@id": new_dict['@id'] + '_geometry',
                "@type": "http://www.opengis.net/ont/sf#Point",
                "http://www.opengis.net/ont/geosparql#asWKT": {
                    "@value": f"<http://www.opengis.net/def/crs/EPSG/9.9.1/31370> Point(NaN NaN NaN)",
                    "@type": "http://www.opengis.net/ont/geosparql#wktLiteral"
                }}

        return new_dict
