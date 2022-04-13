from typing import Union, Tuple, Optional, List

from pymarc import Record, Field


def get_values_by_field(pymarc_rcd: Record,
                        field: str) -> List[str]:

    return [v.value() for v in pymarc_rcd.get_fields(field)]


def get_values_by_field_and_subfield(pymarc_record_or_field: Union[Record, Field],
                                     field_and_subfields: Tuple[Optional[str], List[str]]) -> List[str]:
    """
    Get values from whole record: by field or by field and subfield;
    or get values directly from field or from field and subfield.
    Returns list of values or empty list.
    """

    values_to_return = []
    field, subfields = field_and_subfields[0], field_and_subfields[1]

    if type(pymarc_record_or_field) == Record:
        field, subfields = field_and_subfields[0], field_and_subfields[1]

        if subfields:
            if field in pymarc_record_or_field:
                raw_objects_fields_list = pymarc_record_or_field.get_fields(field)

                for raw_object_field in raw_objects_fields_list:
                    to_append = ' '.join(subfield for subfield in raw_object_field.get_subfields(*subfields))
                    if to_append:
                        values_to_return.append(to_append)
        else:
            if field in pymarc_record_or_field:
                for value in pymarc_record_or_field.get_fields(field):
                    values_to_return.append(value.value())

    else:
        to_append = ' '.join(subfield for subfield in pymarc_record_or_field.get_subfields(*subfields))
        if to_append:
            values_to_return.append(to_append)

    return values_to_return


def get_language_of_original(pymarc_rcd: Record) -> list:
    language_orig = ''

    lang_008 = get_values_by_field(pymarc_rcd, '008')[0][35:38]
    lang_041_h = get_values_by_field_and_subfield(pymarc_rcd, ('041', ['h']))

    if lang_008 and not lang_041_h:
        language_orig = [lang_008]
    if len(lang_041_h) == 1 and len(lang_041_h[0]) == 3:
        language_orig = lang_041_h
    if len(lang_041_h) > 1 and len(lang_041_h[0]) == 3:
        language_orig = lang_041_h[0].split(' ')
    if len(lang_041_h) == 1 and len(lang_041_h[0]) > 3:
        language_orig = lang_041_h[0].split(' ')
    else:
        language_orig = lang_041_h

    return language_orig


def get_publication_dates(pymarc_rcd) -> Optional[int]:
    publication_date_single = None
    publication_date_from = None
    publication_date_to = None

    try:
        v_008_06 = get_values_by_field(pymarc_rcd, '008')[0][6]
    except IndexError:
        v_008_06 = None

    if v_008_06:
        if v_008_06 in ['r', 's', 'p', 't']:
            v_008_0710 = get_values_by_field(pymarc_rcd,
                                             '008')[0][7:11].replace('u', '0').replace(' ', '0').replace('X', '0')
            try:
                publication_date_single = int(v_008_0710)
            except ValueError:
                pass
        else:
            v_008_0710 = get_values_by_field(pymarc_rcd,
                                             '008')[0][7:11].replace('u', '0').replace(' ', '0').replace('X', '0')
            v_008_1114 = get_values_by_field(pymarc_rcd,
                                             '008')[0][11:15].replace('u', '0').replace(' ', '0').replace('X', '0')
            try:
                publication_date_from = int(v_008_0710)
                publication_date_to = int(v_008_1114)
                if publication_date_to == 9999:
                    publication_date_to = None
            except ValueError:
                pass

    return publication_date_single


def get_title_of_original(pymarc_rcd):
    return ''
