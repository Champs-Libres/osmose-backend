#!/usr/bin/env python
#-*- coding: utf-8 -*-

###########################################################################
##                                                                       ##
## Copyrights Francesco Ansanelli 2020                                   ##
##                                                                       ##
## This program is free software: you can redistribute it and/or modify  ##
## it under the terms of the GNU General Public License as published by  ##
## the Free Software Foundation, either version 3 of the License, or     ##
## (at your option) any later version.                                   ##
##                                                                       ##
## This program is distributed in the hope that it will be useful,       ##
## but WITHOUT ANY WARRANTY; without even the implied warranty of        ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         ##
## GNU General Public License for more details.                          ##
##                                                                       ##
## You should have received a copy of the GNU General Public License     ##
## along with this program.  If not, see <http://www.gnu.org/licenses/>. ##
##                                                                       ##
###########################################################################

from modules.OsmoseTranslation import T_
from .Analyser_Merge import Analyser_Merge, Source, CSV, Load, Mapping, Select, Generate
from modules import italian_strings


class Analyser_Merge_Pharmacy_IT(Analyser_Merge):
    def __init__(self, config, logger = None):
        Analyser_Merge.__init__(self, config, logger)
        self.def_class_missing_official(item = 8210, id = 11, level = 3, tags = ['merge', 'highway'],
            title = T_('Pharmacy not integrated'))
        self.def_class_missing_osm(item = 7150, id = 12, level = 3, tags = ['merge', 'highway'],
            title = T_('Pharmacy without tag `ref:msal` or invalid'))
        self.def_class_possible_merge(item = 8211, id = 13, level = 3, tags = ['merge', 'highway'],
            title = T_('Pharmacy integration suggestion'))
        self.def_class_update_official(item = 8212, id = 14, level = 3, tags = ['merge', 'highway'],
            title = T_('Pharmacy update'))

        self.init(
            'http://www.dati.salute.gov.it/dataset/farmacie.jsp',
            'Ministero della Salute',
            CSV(Source(attribution = 'Ministero della Salute', fileUrl = 'http://www.dati.salute.gov.it/imgs/C_17_dataset_5_download_itemDownload0_upFile.CSV'),
                separator = ';'),
            Load('LONGITUDINE', 'LATITUDINE',
                xFunction = self.float_comma,
                yFunction = self.float_comma,
                where = lambda row: row['DATAFINEVALIDITA'] == '-' and row['LONGITUDINE'] != '-' and row['LATITUDINE'] != '-'),
            Mapping(
                select = Select(
                    types = ['nodes', 'ways'],
                    tags = {
                        'amenity': 'pharmacy',
                        'dispensing': 'yes'}),
                osmRef = 'ref:msal',
                conflationDistance = 80,
                generate = Generate(
                    static1 = {
                        'amenity': 'pharmacy',
                        'dispensing': 'yes'},
                    static2 = {'source': self.source},
                    mapping1 = {
                        'ref:msal': 'CODICEIDENTIFICATIVOFARMACIA',
                        'ref:vatin': lambda res: italian_strings.osmRefVatin(res['PARTITAIVA'])
                    },
                    mapping2 = {
                        'operator': lambda res: italian_strings.normalize_pharmacy(res['DESCRIZIONEFARMACIA']),
                        'source:start_date': lambda res: self.date_format(res['DATAINIZIOVALIDITA'])
                    },
                text = lambda tags, fields: {'en': '{0}, {1}'.format(fields['INDIRIZZO'], fields['DESCRIZIONECOMUNE'])} )))
