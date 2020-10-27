import itertools
from queue import Queue

from config import (
    TABLE as table,
    OSM_TABLE as osm_table
)
from subregions import (
    get_subregions_info,
    update_border_mwm_size_estimation,
)


# admin_level => list of countries which should be initially divided at one admin level
unilevel_countries = {
        2: [
            'Afghanistan',
            'Albania',
            'Algeria',
            'Andorra',
            'Angola',
            'Antigua and Barbuda',
            'Armenia',
            'Australia', # need to be divided at level 4 but has many small islands of level 4
            'Azerbaijan', # has 2 non-covering 3-level regions
            'Bahrain',
            'Barbados',
            'Belize',
            'Benin',
            'Bermuda',
            'Bhutan',
            'Botswana',
            'British Sovereign Base Areas',  # ! include into Cyprus
            'British Virgin Islands',
            'Bulgaria',
            'Burkina Faso',
            'Burundi',
            'Cambodia',
            'Cameroon',
            'Cape Verde',
            'Central African Republic',
            'Chad',
            'Chile',
            'Colombia',
            'Comoros',
            'Congo-Brazzaville',  # BUG whith autodivision at level 4
            'Cook Islands',
            'Costa Rica',
            'Croatia',  # next level = 6
            'Cuba',
            'Cyprus',
            "Côte d'Ivoire",
            'Democratic Republic of the Congo',
            'Djibouti',
            'Dominica',
            'Dominican Republic',
            'East Timor',
            'Ecuador',
            'Egypt',
            'El Salvador',
            'Equatorial Guinea',
            'Eritrea',
            'Estonia',
            'Eswatini',
            'Ethiopia',
            'Falkland Islands',
            'Faroe Islands',
            'Federated States of Micronesia',
            'Fiji',
            'Gabon',
            'Georgia',
            'Ghana',
            'Gibraltar',
            'Greenland',
            'Grenada',
            'Guatemala',
            'Guernsey',
            'Guinea',
            'Guinea-Bissau',
            'Guyana',
            'Haiti',
            'Honduras',
            'Iceland',
            'Indonesia',
            'Iran',
            'Iraq',
            'Isle of Man',
            'Israel',  # ! don't forget to separate Jerusalem
            'Jamaica',
            'Jersey',
            'Jordan',
            'Kazakhstan',
            'Kenya',  # ! level 3 doesn't cover the whole country
            'Kiribati',
            'Kosovo',
            'Kuwait',
            'Kyrgyzstan',
            'Laos',
            'Latvia',
            'Lebanon',
            'Liberia',
            'Libya',
            'Liechtenstein',
            'Lithuania',
            'Luxembourg',
            'Madagascar',
            'Malaysia',
            'Maldives',
            'Mali',
            'Malta',
            'Marshall Islands',
            'Martinique',
            'Mauritania',
            'Mauritius',
            'Mexico',
            'Moldova',
            'Monaco',
            'Mongolia',
            'Montenegro',
            'Montserrat',
            'Mozambique',
            'Myanmar',
            'Namibia',
            'Nauru',
            'Nicaragua',
            'Niger',
            'Nigeria',
            'Niue',
            'North Korea',
            'North Macedonia',
            'Oman',
            'Palau',
            # ! 'Palestina' is not a country in OSM - need make an mwm
            'Panama',
            'Papua New Guinea',
            'Peru', #  need split-merge
            'Philippines',  # split at level 3 and merge or not merge
            'Qatar',
            'Romania', #  need split-merge
            'Rwanda',
            'Saint Helena, Ascension and Tristan da Cunha',
            'Saint Kitts and Nevis',
            'Saint Lucia',
            'Saint Vincent and the Grenadines',
            'San Marino',
            'Samoa',
            'Saudi Arabia',
            'Senegal',
            'Seychelles',
            'Sierra Leone',
            'Singapore',
            'Slovakia', # ! split at level 3 then 4, and add Bratislava region (4)
            'Slovenia',
            'Solomon Islands',
            'Somalia',
            'South Georgia and the South Sandwich Islands',
            'South Korea',
            'South Sudan',
            'South Ossetia',  # ! don't forget to divide from Georgia
            'Sri Lanka',
            'Sudan',
            'São Tomé and Príncipe',
            'Suriname',
            'Switzerland',
            'Syria',
            'Taiwan',
            'Tajikistan',
            'Thailand',
            'The Bahamas',
            'The Gambia',
            'Togo',
            'Tokelau',
            'Tonga',
            'Trinidad and Tobago',
            'Tunisia',
            'Turkmenistan',
            'Turks and Caicos Islands',
            'Tuvalu',
            'United Arab Emirate',
            'Uruguay',
            'Uzbekistan',
            'Vanuatu',
            'Venezuela', # level 3 not comprehensive
            'Vietnam',
            # ! don't forget 'Wallis and Futuna', belongs to France
            'Yemen',
            'Zambia',
            'Zimbabwe',
           ],
        3: [
            'Malawi',
            'Nepal',  # ! one region is lost after division
            'Pakistan',
            'Paraguay',
            'Tanzania',
            'Turkey',
            'Uganda',
           ],
        4: [
            'Austria',
            'Bangladesh',
            'Belarus',  # maybe need merge capital region with the province
            'Belgium',  # maybe need merge capital region into encompassing province
            'Bolivia',
            'Bosnia and Herzegovina', # other levels - 5, 6, 7 - are incomplete.
            'Canada',
            'China',  # ! don't forget about Macau and Hong Kong of level 3 not covered by level 4
            'Denmark',
            'Greece',  # ! has one small 3-level subregion!
            'Hungary',  # maybe multilevel division at levels [4, 5] ?
            'India',
            'Italy',
            'Japan',  # ? About 50 4-level subregions, some of which requires further division
            'Morocco',  # ! not all regions appear after substitution with level 4
            'New Zealand',  # ! don't forget islands to the north and south
            'Norway',
            'Poland',  # 380(!) subregions of AL=6
            'Portugal',
            'Russia',
            'Serbia',
            'South Africa',
            'Spain',
            'Ukraine',
            'United States',
           ],
        5: [
            'Ireland',  # ! 5-level don't cover the whole country
           ],
        6: [
            'Czechia',
           ]
}

# Country name => list of admin levels to which it should be initially divided.
# 'Germany': [4, 5] implies that the country is divided at level 4 at first, then all
#  4-level subregions are divided into subregions of level 5 (if any)
multilevel_countries = {
        'Brazil': [3, 4],
        'Finland': [3, 6], # [3,5,6] in more fresh data?   # division by level 6 seems ideal
        'France': [3, 4],
        'Germany': [4, 5],  # not the whole country is covered by units of AL=5
        'Netherlands': [3, 4], # there are carribean lands of level both 3 and 4
        'Sweden': [3, 4],  # division by level 4 seems ideal
        'United Kingdom': [4, 5],  # level 5 is necessary but not comprehensive

}

country_initial_levels = dict(itertools.chain(
    ((country, ([level] if level > 2 else []))
        for level, countries in unilevel_countries.items()
        for country in countries),
    multilevel_countries.items()
))


class CountryStructureException(Exception):
    pass


def _clear_borders(conn):
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM {table}")
    conn.commit()


def _find_subregions(conn, osm_ids, next_level, regions):
    """Return subregions of level 'next_level' for regions with osm_ids."""
    subregion_ids = []
    for osm_id in osm_ids:
        more_subregions = get_subregions_info(conn, osm_id, table,
                                              next_level, need_cities=False)
        for subregion_id, subregion_data in more_subregions.items():
            region_data = regions.setdefault(subregion_id, {})
            region_data['name'] = subregion_data['name']
            region_data['mwm_size_est'] = subregion_data['mwm_size_est']
            region_data['parent_id'] = osm_id
            subregion_ids.append(subregion_id)
    return subregion_ids


def _create_regions(conn, osm_ids, regions):
    if not osm_ids:
        return
    osm_ids = list(osm_ids)  # to ensure order
    cursor = conn.cursor()
    sql_values = ','.join(
            f'({osm_id},'
            '%s,'
            f"{regions[osm_id].get('parent_id', 'NULL')},"
            f"{regions[osm_id].get('mwm_size_est', 'NULL')},"
            f'(SELECT way FROM {osm_table} WHERE osm_id={osm_id}),'
            'now())'
            for osm_id in osm_ids
    )
    #print(f"create regions with osm_ids={osm_ids}")
    #print(f"names={tuple(names[osm_id] for osm_id in osm_ids)}")
    #print(f"all parents={parents}")
    cursor.execute(f"""
        INSERT INTO {table} (id, name, parent_id, mwm_size_est, geom, modified)
        VALUES {sql_values}
        """, tuple(regions[osm_id]['name'] for osm_id in osm_ids)
    )


def _make_country_structure(conn, country_osm_id):
    regions = {}  # osm_id: { 'name': name,
                  #           'mwm_size_est': size,
                  #           'parent_id': parent_id }

    country_name = get_osm_border_name_by_osm_id(conn, country_osm_id)
    country_data = regions.setdefault(country_osm_id, {})
    country_data['name'] = country_name
    # TODO: country_data['mwm_size_est'] = ...

    _create_regions(conn, [country_osm_id], regions)

    if country_initial_levels.get(country_name):
        admin_levels = country_initial_levels[country_name]
        prev_admin_levels = [2] + admin_levels[:-1]
        prev_region_ids = [country_osm_id]

        for admin_level, prev_level in zip(admin_levels, prev_admin_levels):
            if not prev_region_ids:
                raise CountryStructureException(
                        f"Empty prev_region_ids at {country_name}, "
                        f"AL={admin_level}, prev-AL={prev_level}"
                )
            subregion_ids = _find_subregions(conn, prev_region_ids,
                                             admin_level, regions)
            _create_regions(conn, subregion_ids, regions)
            prev_region_ids = subregion_ids
    warning = None
    if len(regions) == 1:
        try:
            update_border_mwm_size_estimation(conn, country_osm_id)
        except Exception as e:
            warning = str(e)
    return warning


def create_countries_initial_structure(conn):
    _clear_borders(conn)
    cursor = conn.cursor()
    # TODO: process overlapping countries, like Ukraine and Russia with common Crimea
    cursor.execute(f"""
        SELECT osm_id, name
        FROM {osm_table}
        WHERE admin_level = 2 and name != 'Ukraine'
        """
        #  and name in --('Germany', 'Luxembourg', 'Austria')
        #    ({','.join(f"'{c}'" for c in country_initial_levels.keys())})
        #"""
    )
    warnings = []
    for rec in cursor:
        warning = _make_country_structure(conn, rec[0])
        if warning:
            warnings.append(warning)
    conn.commit()
    return warnings


def get_osm_border_name_by_osm_id(conn, osm_id):
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT name FROM {osm_table}
        WHERE osm_id = %s
        """, (osm_id,))
    rec = cursor.fetchone()
    if not rec:
        raise CountryStructureException(f'Not found region with osm_id="{osm_id}"')
    return rec[0]


def _get_country_osm_id_by_name(conn, name):
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT osm_id FROM {osm_table}
        WHERE admin_level = 2 AND name = %s
        """, (name,))
    row_count = cursor.rowcount
    if row_count > 1:
        raise CountryStructureException(f'More than one country "{name}"')
    rec = cursor.fetchone()
    if not rec:
        raise CountryStructureException(f'Not found country "{name}"')
    return int(rec[0])


def is_administrative_region(conn, region_id):
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT count(1) FROM {osm_table} WHERE osm_id = %s
        """, (region_id,)
    )
    count = cursor.fetchone()[0]
    return (count > 0)


def find_osm_child_regions(conn, region_id):
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT c.id, oc.admin_level
        FROM {table} c, {table} p, {osm_table} oc
        WHERE p.id = c.parent_id AND c.id = oc.osm_id
            AND p.id = %s
        """, (region_id,)
    )
    children = []
    for rec in cursor:
        children.append({'id': int(rec[0]), 'admin_level': int(rec[1])})
    return children


def is_leaf(conn, region_id):
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT count(1)
        FROM {table}
        WHERE parent_id = %s
        """, (region_id,)
    )
    count = cursor.fetchone()[0]
    return (count == 0)


def get_region_country(conn, region_id):
    """Returns the uppermost predecessor of the region in the hierarchy,
    possibly itself.
    """
    predecessors = get_predecessors(conn, region_id)
    return predecessors[-1]


def get_predecessors(conn, region_id):
    """Returns the list of (id, name)-tuples of all predecessors,
    starting from the very region_id.
    """
    predecessors = []
    cursor = conn.cursor()
    while True:
        cursor.execute(f"""
            SELECT id, name, parent_id
            FROM {table} WHERE id={region_id}
            """
        )
        rec = cursor.fetchone()
        if not rec:
           raise Exception(f"No record in '{table}' table with id = {region_id}")
        predecessors.append(rec[0:2])
        parent_id = rec[2]
        if not parent_id:
            break
        region_id = parent_id
    return predecessors


def get_region_full_name(conn, region_id):
    predecessors = get_predecessors(conn, region_id)
    return '_'.join(pr[1] for pr in reversed(predecessors))


def get_similar_regions(conn, region_id, only_leaves=False):
    """Returns ids of regions of the same admin_level in the same country.
    Prerequisite: is_administrative_region(region_id) is True.
    """
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT admin_level FROM {osm_table}
        WHERE osm_id = %s""", (region_id,)
    )
    admin_level = int(cursor.fetchone()[0])
    country_id, country_name = get_region_country(conn, region_id)
    q = Queue()
    q.put({'id': country_id, 'admin_level': 2})
    similar_region_ids = []
    while not q.empty():
        item = q.get()
        if item['admin_level'] == admin_level:
            similar_region_ids.append(item['id'])
        elif item['admin_level'] < admin_level:
            children = find_osm_child_regions(item['id'])
            for ch in children:
                q.put(ch)
    if only_leaves:
        similar_region_ids = [r_id for r_id in similar_region_ids
                                  if is_leaf(conn, r_id)]
    return similar_region_ids
