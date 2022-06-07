import re
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
    language_orig = []

    lang_008 = get_values_by_field(pymarc_rcd, '008')[0][35:38]
    lang_041_h = get_values_by_field_and_subfield(pymarc_rcd, ('041', ['h']))

    if lang_008 and not lang_041_h:
        language_orig = [lang_008]
    if lang_041_h:
        if len(lang_041_h) == 1 and len(lang_041_h[0]) == 3:
            language_orig = lang_041_h
        if len(lang_041_h) > 1 and len(lang_041_h[0]) == 3:
            language_orig = lang_041_h[0].split(' ')
        if len(lang_041_h) == 1 and len(lang_041_h[0]) > 3:
            language_orig = lang_041_h[0].split(' ')
    else:
        language_orig = lang_041_h

    return language_orig


def get_language_of_publication(pymarc_rcd: Record) -> list:
    language_of_publication = []

    lang_from_008 = get_values_by_field(pymarc_rcd, '008')[0][35:38]
    lang_from_041_a = get_values_by_field_and_subfield(pymarc_rcd, ('041', ['a']))

    if lang_from_008:
        language_of_publication = [lang_from_008]
        return language_of_publication
    if lang_from_041_a:
        language_of_publication = lang_from_041_a
        return language_of_publication

    return language_of_publication


def is_translation(pymarc_rcd: Record) -> bool:
    result = False

    val_700_e = get_values_by_field_and_subfield(pymarc_rcd, ('700', ['e']))
    val_700_e_joined = ''.join(val_700_e)
    val_700_e_joined = val_700_e_joined.upper()

    val_245_c = get_values_by_field_and_subfield(pymarc_rcd, ('245', ['c']))
    val_245_c_joined = ''.join(val_245_c)
    val_245_c_joined = val_245_c_joined.upper()

    transl_700_e = False
    if 'TŁ' in val_700_e_joined or 'PRZEKŁ' in val_700_e_joined or 'PRZEŁ' in val_700_e_joined:
        transl_700_e = True
    if transl_700_e and 'STRESZCZ' not in val_245_c_joined:
        result = True

    transl_245_c = False
    if 'TŁ.' in val_245_c_joined or 'PRZEKŁ.' in val_245_c_joined or 'PRZEŁ.' in val_245_c_joined \
            or 'TŁUM.' in val_245_c_joined or 'TŁUMACZENIE' in val_245_c_joined or 'PRZEKŁAD' in val_245_c_joined \
            or 'PRZEŁOŻYŁ' in val_245_c_joined:
        transl_245_c = True
    if transl_245_c and 'STRESZCZ' not in val_245_c_joined:
        result = True

    return result


def get_publication_dates(pymarc_rcd) -> Optional[int]:
    publication_date_single = None
    publication_date_from = None
    publication_date_to = None
    publication_date_from_260 = None

    val_260c = get_values_by_field_and_subfield(pymarc_rcd, ('260', ['c']))
    try:
        if val_260c:
            val_260c = val_260c[0]
            extracted_date = ''.join(gr for gr in re.findall(r'\d', val_260c))
            if extracted_date:
                val_260c = extracted_date
                publication_date_from_260 = val_260c
    except Exception:
        pass

    try:
        v_008_06 = get_values_by_field(pymarc_rcd, '008')[0][6]
    except IndexError:
        v_008_06 = None

    if v_008_06:
        if v_008_06 in ['r', 's', 'p', 't']:
            try:
                v_008_0710 = get_values_by_field(pymarc_rcd,
                                                 '008')[0][7:11].replace('u', '0').replace(' ', '0').replace('X', '0')
            except IndexError:
                v_008_0710 = None

            try:
                publication_date_single = int(v_008_0710)
            except ValueError:
                pass
        else:
            try:
                v_008_0710 = get_values_by_field(pymarc_rcd,
                                                 '008')[0][7:11].replace('u', '0').replace(' ', '0').replace('X', '0')
                v_008_1114 = get_values_by_field(pymarc_rcd,
                                                 '008')[0][11:15].replace('u', '0').replace(' ', '0').replace('X', '0')
            except IndexError:
                v_008_0710 = None
                v_008_1114 = None

            try:
                publication_date_from = int(v_008_0710)
                publication_date_to = int(v_008_1114)
                if publication_date_to == 9999:
                    publication_date_to = None
                else:
                    publication_date_single = publication_date_to
            except ValueError:
                pass

    if not publication_date_single and publication_date_from_260:
        publication_date_single = publication_date_from_260

    return publication_date_single


def get_title_of_original(pymarc_rcd):
    orig_title_i_list = ['Tyt. oryg.:', 'Tyt. oryg.', 'Tyt. oryg', 'Tyt.oryg.:', 'Tyt.oryg.', 'Tyt.oryg',
                         'Tytuł oryginału:', 'Tytuł oryginału', 'Tytułoryginału', 'Przekład z:']

    title_of_original_final = ''

    titles_from_246_raw_fields = pymarc_rcd.get_fields('246')
    title_of_original_raw_value = ''
    for title_246_raw_field in titles_from_246_raw_fields:
        if title_246_raw_field.get_subfields('i') and title_246_raw_field.get_subfields('i')[0] in orig_title_i_list:
            title_of_original_raw_value = title_246_raw_field.get_subfields('a', 'b', 'n', 'p')[0]
            break

    if title_of_original_raw_value:
        # get rid of publication date from title from 246
        match = re.search(r'\s*,\s*\d+$', title_of_original_raw_value)
        if match:
            title_of_original_final = title_of_original_raw_value[:match.span(0)[0]]
        else:
            title_of_original_final = title_of_original_raw_value
    title_of_original_final = title_of_original_final.rstrip('/,:;.').strip()

    return title_of_original_final


def get_audience_characteristics(pymarc_rcd) -> list:
    audience_characteristics_final = []

    audience_characteristics_raw_fields = pymarc_rcd.get_fields('385')
    for audience_characteristics_raw_field in audience_characteristics_raw_fields:
        if audience_characteristics_raw_field.get_subfields('m') \
                and 'Grupa wiekowa' in audience_characteristics_raw_field.get_subfields('m')[0]:
            audience_characteristics_raw_value = audience_characteristics_raw_field.get_subfields('a')[0]
            audience_characteristics_final.append(audience_characteristics_raw_value)

    return audience_characteristics_final


def get_publisher_uniform_name(pymarc_rcd) -> list:
    publisher_uniform_name_final = []

    publisher_uniform_name_raw_fields = pymarc_rcd.get_fields('710')
    for publisher_uniform_name_raw_field in publisher_uniform_name_raw_fields:
        if publisher_uniform_name_raw_field.get_subfields('4') \
                and 'pbl' in publisher_uniform_name_raw_field.get_subfields('4')[0]:
            publisher_uniform_name_raw_value = publisher_uniform_name_raw_field.get_subfields('a')[0]
            publisher_uniform_name_final.append(publisher_uniform_name_raw_value)

    return publisher_uniform_name_final


def get_creator(pymarc_rcd) -> list:
    creators_final = []

    creators_raw_fields = pymarc_rcd.get_fields('100')
    for creator_raw_field in creators_raw_fields:
        if creator_raw_field.get_subfields('e'):
            creator_responsibilities = ' '.join(creator_raw_field.get_subfields('e'))
            creator_raw_value = creator_raw_field.get_subfields('a', 'b', 'c', 'd', 'n')[0].rstrip('.').strip()
            creator_final = f'{creator_raw_value} ({creator_responsibilities})'
            creators_final.append(creator_final)
        else:
            creator_raw_value = creator_raw_field.get_subfields('a', 'b', 'c', 'd', 'n')[0]
            creators_final.append(creator_raw_value)

    return creators_final


def get_cocreator(pymarc_rcd) -> list:
    cocreators_final = []

    cocreators_raw_fields = pymarc_rcd.get_fields('700')
    for cocreator_raw_field in cocreators_raw_fields:
        if cocreator_raw_field.get_subfields('e'):
            cocreator_responsibilities = ' '.join(cocreator_raw_field.get_subfields('e'))
            cocreator_raw_value = cocreator_raw_field.get_subfields('a', 'b', 'c', 'd', 'n')[0].rstrip('.').strip()
            cocreator_final = f'{cocreator_raw_value} ({cocreator_responsibilities})'
            cocreators_final.append(cocreator_final)
        else:
            cocreator_raw_value = cocreator_raw_field.get_subfields('a', 'b', 'c', 'd', 'n')[0]
            cocreators_final.append(cocreator_raw_value)

    return cocreators_final

