import itertools


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
