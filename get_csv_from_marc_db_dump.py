import logging
import sys
import csv

from pymarc import MARCReader

from commons.configuration_loader import load_config
from commons.downloaders.db_dump_downloader import get_raw_db

import commons.marc_handling.attributes_extractors as attr_extr

marc_reader_logger = logging.getLogger('marc_reader')


PROGRESS_UPDATE_STEP = 10000


class MARC2csvDataModel(object):
    def __init__(self,
                 mms_id, publication_date, publication_country, isbn, language_of_original,
                 language_of_intermediate_translation, udc, other_classification_number,
                 creator, title, title_of_original, edition, publication_place, extent, form_of_work,
                 audience_characteristics, contributor_characteristics, genre, cocreator, cocreator_only_translator,
                 cocreator_without_translator,
                 publisher_uniform_name, series_personal, series_title, is_selected_value):
        self.data = {'mms_id': mms_id,
                     'publication_date': publication_date,
                     'publication_country': publication_country,
                     'isbn': isbn,
                     'language_of_original': language_of_original,
                     'language_of_intermediate_translation': language_of_intermediate_translation,
                     'udc': udc,
                     'other_classification_number': other_classification_number,
                     'creator': creator,
                     'title': title,
                     'title_of_original': title_of_original,
                     'edition': edition,
                     'publication_place': publication_place,
                     'extent': extent,
                     'form_of_work': form_of_work,
                     'audience_characteristics': audience_characteristics,
                     'contributor_characteristics': contributor_characteristics,
                     'genre': genre,
                     'cocreator': cocreator,
                     'cocreator_only_translator': cocreator_only_translator,
                     'cocreator_without_translator': cocreator_without_translator,
                     'publisher_uniform_name': publisher_uniform_name,
                     'series_personal': series_personal,
                     'series_title': series_title,
                     'is_selected_value': is_selected_value}

    def as_sanitized_for_csv_dict(self) -> dict[str: str]:
        sanitized_for_csv_dict = {}
        for key, value in self.data.items():
            if value and type(value) is list:
                sanitized_for_csv_dict[key] = '|'.join(value)
            if value and (type(value) is str or type(value) is int):
                sanitized_for_csv_dict[key] = value
            if not value:
                sanitized_for_csv_dict[key] = ''

        return sanitized_for_csv_dict


def is_selected(pymarc_rcd) -> int:
    publication_date = attr_extr.get_publication_dates(pymarc_rcd)
    language_of_original = attr_extr.get_language_of_original(pymarc_rcd)
    language_of_publication = ''.join(attr_extr.get_language_of_publication(pymarc_rcd))
    field_041h = attr_extr.get_values_by_field_and_subfield(pymarc_rcd, ('041', ['h']))
    form_of_work = attr_extr.get_values_by_field_and_subfield(pymarc_rcd, ('380', ['a']))
    is_translation = attr_extr.is_translation(pymarc_rcd)
    genre_of_work = attr_extr.get_values_by_field_and_subfield(pymarc_rcd, ('655', ['a']))
    #genre_general_subdivision = attr_extr.get_values_by_field_and_subfield(pymarc_rcd, ('655', ['x']))

    try:
        if publication_date >= 1918 \
                and language_of_original != 'pol' and field_041h \
                and 'pol' in language_of_publication \
                and ('Książki' in form_of_work or "E-booki" in form_of_work or ('Artykuły' in form_of_work and 'Nadbitki i odbitki' in genre_of_work)):
            return 1
        if publication_date >= 1918 \
                and language_of_original != 'pol' and (is_translation or field_041h) \
                and 'pol' in language_of_publication \
                and (('Książki' in form_of_work or "E-booki" in form_of_work or ('Artykuły' in form_of_work and 'Nadbitki i odbitki' in genre_of_work)) or not form_of_work):
            return 2
        else:
            return 0
    except Exception:
        return 0


def extract_to_csv(pymarc_rcd, is_selected_value):
    mms_id = attr_extr.get_values_by_field(pymarc_rcd, '009')[0]
    publication_date = attr_extr.get_publication_dates(pymarc_rcd)
    publication_country = attr_extr.get_country_of_publication(pymarc_rcd)
    isbn = attr_extr.get_values_by_field_and_subfield(pymarc_rcd, ('020', ['a']))
    language_of_original = attr_extr.get_language_of_original(pymarc_rcd)
    language_of_intermediate_translation = attr_extr.get_values_by_field_and_subfield(pymarc_rcd, ('041', ['k']))
    udc = attr_extr.get_values_by_field_and_subfield(pymarc_rcd, ('080', ['a']))
    other_classification_number = attr_extr.get_values_by_field_and_subfield(pymarc_rcd, ('084', ['a']))
    creator = attr_extr.get_creator(pymarc_rcd)
    title = attr_extr.get_values_by_field_and_subfield(pymarc_rcd,
                                                       ('245', ['a', 'b', 'n', 'p']))[0].rstrip('/;:=,.').strip()
    title_of_original = attr_extr.get_title_of_original(pymarc_rcd)
    edition = attr_extr.get_values_by_field_and_subfield(pymarc_rcd,
                                                         ('250', ['a']))[0].rstrip('/').strip() if attr_extr.get_values_by_field_and_subfield(pymarc_rcd, ('250', ['a'])) else ''
    publication_place = attr_extr.get_values_by_field_and_subfield(pymarc_rcd,
                                                                   ('260', ['a']))[0].strip().rstrip(':').strip().replace('[etc.]', '').replace('[', '').replace(']', '').replace(' : ', ' ; ').split(' ; ') if attr_extr.get_values_by_field_and_subfield(pymarc_rcd, ('260', ['a'])) else ''
    extent = attr_extr.get_values_by_field_and_subfield(pymarc_rcd,
                                                        ('300', ['a']))[0].rstrip(':;').strip() if attr_extr.get_values_by_field_and_subfield(pymarc_rcd, ('300', ['a'])) else ''
    form_of_work = attr_extr.get_values_by_field_and_subfield(pymarc_rcd,
                                                              ('380', ['a']))
    audience_characteristics = attr_extr.get_audience_characteristics(pymarc_rcd)
    contributor_characteristics = attr_extr.get_values_by_field_and_subfield(pymarc_rcd,
                                                                             ('386', ['a']))
    genre = attr_extr.get_values_by_field_and_subfield(pymarc_rcd,
                                                       ('655', ['a']))
    cocreator = attr_extr.get_cocreator(pymarc_rcd)
    cocreator_only_translator = [translator for translator in cocreator if '[Tł' in translator]
    cocreator_without_translator = [ccr for ccr in cocreator if '[Tł' not in ccr]
    publisher_uniform_name = attr_extr.get_publisher_uniform_name(pymarc_rcd)
    series_personal = attr_extr.get_values_by_field_and_subfield(pymarc_rcd,
                                                                 ('800', ['a', 't', 'v']))
    series_title = attr_extr.get_values_by_field_and_subfield(pymarc_rcd,
                                                              ('830', ['a', 'x', 'v']))

    return MARC2csvDataModel(mms_id=mms_id,
                             publication_date=publication_date,
                             publication_country=publication_country,
                             isbn=isbn,
                             language_of_original=language_of_original,
                             language_of_intermediate_translation=language_of_intermediate_translation,
                             udc=udc,
                             other_classification_number=other_classification_number,
                             creator=creator,
                             title=title,
                             title_of_original=title_of_original,
                             edition=edition,
                             publication_place=publication_place,
                             extent=extent,
                             form_of_work=form_of_work,
                             audience_characteristics=audience_characteristics,
                             contributor_characteristics=contributor_characteristics,
                             genre=genre,
                             cocreator=cocreator,
                             cocreator_only_translator=cocreator_only_translator,
                             cocreator_without_translator=cocreator_without_translator,
                             publisher_uniform_name=publisher_uniform_name,
                             series_personal=series_personal,
                             series_title=series_title,
                             is_selected_value=is_selected_value)


def select_and_extract_records_to_csv(path_to_raw_db):
    with open(path_to_raw_db, 'rb') as fp:
        rdr = MARCReader(fp, to_unicode=True, force_utf8=True, utf8_handling='ignore', permissive=True)

        counter = 0
        for rcd in rdr:
            if counter % PROGRESS_UPDATE_STEP == 0:
                marc_reader_logger.info(f'Processed {counter} records.')
            counter += 1

            is_selected_value = is_selected(rcd)
            if is_selected_value == 1 or is_selected_value == 2:
                try:
                    exctracted_to_csv = extract_to_csv(rcd, is_selected_value)
                    yield exctracted_to_csv
                except Exception as e:
                    if rcd.get_fields('001') and rcd.get_fields('009'):
                        marc_reader_logger.error(f"{e} || {rcd.get_fields('001')[0].value()}, {rcd.get_fields('009')[0].value()}")
                    yield ''


def dump_to_csv(records_buffer):
    with open('extracted_csv.csv', 'a', newline='', encoding='utf-8') as fp:
        csv_writer = csv.writer(fp, delimiter=',', quoting=csv.QUOTE_ALL)
        for record in records_buffer:
            csv_writer.writerow(record.as_sanitized_for_csv_dict().values())


def main(db_config):
    # get path to db (and download it, if needed)
    path_to_raw_db = get_raw_db(db_config.get('source_db_name'), db_config.get('skip_download'))

    records_buffer = []
    for record in select_and_extract_records_to_csv(path_to_raw_db):
        if record:
            records_buffer.append(record)
            if len(records_buffer) == 500:
                dump_to_csv(records_buffer)
                records_buffer = []
    if records_buffer:
        dump_to_csv(records_buffer)


if __name__ == '__main__':
    # set up logging
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    fhandler = logging.FileHandler(f'root.log', encoding='utf-8')
    strhandler = logging.StreamHandler(sys.stdout)
    fhandler.setFormatter(formatter)
    strhandler.setFormatter(formatter)

    fhandler_2 = logging.FileHandler(f'errors.log', encoding='utf-8')
    fhandler_2.setFormatter(formatter)
    fhandler_2.setLevel(level=logging.ERROR)

    logging.root.addHandler(strhandler)
    logging.root.addHandler(fhandler)
    logging.root.setLevel(level=logging.INFO)

    marc_reader_logger.addHandler(fhandler_2)
    attr_extr.atrributes_extractors_logger.addHandler(fhandler_2)

    # parse configs
    db_config_parsed = load_config('source_db.yaml')

    # run
    main(db_config_parsed)
