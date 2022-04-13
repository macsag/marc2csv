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
                 mms_id, publication_date, isbn, language_of_original, language_of_intermediate_translation,
                 title, title_of_original):
        self.data = {'mms_id': mms_id,
                     'publication_date': publication_date,
                     'isbn': isbn,
                     'language_of_original': language_of_original,
                     'language_of_intermediate_translation': language_of_intermediate_translation,
                     'title': title,
                     'title_of_original': title_of_original}

    def as_sanitized_for_csv_dict(self) -> dict[str: str]:
        sanitized_for_csv_dict = {}
        for key, value in self.data.items():
            if value and type(value) is list:
                sanitized_for_csv_dict[key] = '|'.join(value)
            if value and type(value) is str or type(value) is int:
                sanitized_for_csv_dict[key] = value
            if not value:
                sanitized_for_csv_dict[key] = ''

        return sanitized_for_csv_dict


def is_selected(pymarc_rcd) -> bool:
    publication_date = attr_extr.get_publication_dates(pymarc_rcd)
    language_of_original = attr_extr.get_language_of_original(pymarc_rcd)
    language = attr_extr.get_values_by_field(pymarc_rcd, '008')[0][35:38]
    field_041h = attr_extr.get_values_by_field_and_subfield(pymarc_rcd, ('041', ['h']))
    form_of_work = attr_extr.get_values_by_field_and_subfield(pymarc_rcd, ('380', ['a']))
    genre_of_work = attr_extr.get_values_by_field_and_subfield(pymarc_rcd, ('655', ['a']))
    genre_general_subdivision = attr_extr.get_values_by_field_and_subfield(pymarc_rcd, ('655', ['x']))

    try:
        if publication_date >= 1918 \
          and language_of_original != 'pol' and field_041h \
          and language == 'pol' \
          and ('Książki' in form_of_work or "E-booki" in form_of_work)  \
          and 'Anegdoty' in ' '.join(genre_of_work) and 'historia' not in genre_general_subdivision:
            return True
        else:
            return False
    except Exception:
        return False


def extract_to_csv(pymarc_rcd):
    mms_id = attr_extr.get_values_by_field(pymarc_rcd, '009')[0]
    publication_date = attr_extr.get_publication_dates(pymarc_rcd)
    isbn = attr_extr.get_values_by_field_and_subfield(pymarc_rcd, ('020', ['a']))
    language_of_original = attr_extr.get_language_of_original(pymarc_rcd)
    language_of_intermediate_translation = attr_extr.get_values_by_field_and_subfield(pymarc_rcd, ('245', ['k']))
    title = attr_extr.get_values_by_field_and_subfield(pymarc_rcd, ('245', ['a', 'b', 'n', 'p']))[0].rstrip('/').strip()
    title_of_original = attr_extr.get_title_of_original(pymarc_rcd)

    return MARC2csvDataModel(mms_id=mms_id,
                             publication_date=publication_date,
                             isbn=isbn,
                             language_of_original=language_of_original,
                             language_of_intermediate_translation=language_of_intermediate_translation,
                             title=title,
                             title_of_original=title_of_original)


def select_and_extract_records_to_csv(path_to_raw_db):
    with open(path_to_raw_db, 'rb') as fp:
        rdr = MARCReader(fp, to_unicode=True, force_utf8=True, utf8_handling='ignore', permissive=True)

        counter = 0
        for rcd in rdr:
            if counter % PROGRESS_UPDATE_STEP == 0:
                marc_reader_logger.info(f'Processed {counter} records')
            counter += 1

            if is_selected(rcd):
                exctracted_to_csv = extract_to_csv(rcd)
                print(exctracted_to_csv)
                yield exctracted_to_csv


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
        records_buffer.append(record)
        if len(records_buffer) == 5:
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

    logging.root.addHandler(strhandler)
    logging.root.addHandler(fhandler)
    logging.root.setLevel(level=logging.INFO)

    # parse configs
    db_config_parsed = load_config('source_db.yaml')

    # run
    main(db_config_parsed)
