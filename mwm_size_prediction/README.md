# MWM size prediction model

The web application uses a data-science-based model to predict size MWM files
compiled from the borders. Here described are the efforts that were undertaken
to build such a prediction model. The serialized model resides at `web/app/data/`
in the `model.pkl` and `scaler.pkl` files. Its first variant was trained only
on county-level data and is valid at limited parameters range (see web/app/config.py
for the model limitations). Now we try to extend the model to predict also
province-level regions.

## Data gathering

We chosen countries/regions with dense OSM data and took them as the training
dataset. As a first try Germany, Austria, Belgium and Netherlands where taken 
giving about 950 borders of different admin levels. The sample was found to
be too small for good training.
Then Norway, Switzerland, Ile-de-France of France, Japan, United Kingdom, Belarus,
4 states: California, Texas, New York, Washington &ndash; of the United States
were added.

#### Geographic data gathering

First, with the help of the web app I split the forementioned countries/regions down to the
"county"-subregions in general sense of a "county" &ndash; it's an admin level
which is too small for MWMs, but regions of one level higher are too big,
so that a usual MWM would be a cluster of "counties".

Japan was a special case, so `extract_mwm_geo_data.py` script contains a
function to split the country into subregions.

The `extract_mwm_geo_data.py` script, endowed with a valid connection
to the database with borders, gathers information about all borders of a
given country/region and its descendants: id, parent_id, admin level, name,
full area, land area (so the table with land borders of the planet is necessary),
city/town count and population, hamlet/village count and population.

One should keep in mind that some borders may be absent in OSM, so a region may
not be fully covered by subregions. So, a region area (or places cout,
or population) may be greater than the sum of areas of its subregions.
One way is to fix borders by hand. Another way, that I followed, is to select
areas, cities and population from the database even for upper-level regions
(except countries, for which the calculation would run too long and is not useful).

#### Mwm size data gathering

Having borders division of the training countries in the web app, I download all
borders, changing the poly-file naming procedure so that the name to contain
the region id. The id would be the link between files with geodata and mwm sizes data.
So we have many border file with names like _03565917_Japan_Gunma Prefecture_Numata.poly_
that I place into the `omim/data/borders/` directory instead of original
borders.

Also, I did a *.o5m-extract for each country to supply the maps_generator
not with the whole planet-latest.o5m file. I used https://boundingbox.klokantech.com 
to find a polygon for an extract, first getting geojson of ten points or so at the
website and then composing a *.poly file in a text editor. With this
`country.poly` file I got a country extract with `osmconvert` tool:
```bash
osmctools/osmconvert planet-latest.o5m -B=country.poly -o=country.o5m
```

Then
```bash
md5sum country.o5m > country.o5m.md5
```
In `maps_generation.ini` I changed the path to the planet and md5sum file and run
the MWMs generation with
```bash
nohup python -m maps_generator --order="" --skip="Routing,RoutingTransit" \
    --without_countries="World*" --countries="*_Switzerland_*" &
```

For the asterisk to work at the beginning of the mask in the `--countries` option,
I made some changes to `omim/tools/generator/maps_generator/__main__.py`:

```python
    def end_star_compare(prefix, full):
        return full.startswith(prefix)

    def start_star_compare(suffix, full):
        return full.endswith(suffix)

    def both_star_compare(substr, full):
        return substr in full

    ...
            cmp = compare
            _raw_country = country_item[:]
            if _raw_country:
                if all(_raw_country[i] == "*" for i in (0, -1)):
                    _raw_country = _raw_country.replace("*", "") 
                    cmp = both_star_compare
                elif _raw_country[-1] == "*":
                    _raw_country = _raw_country.replace("*", "") 
                    cmp = end_star_compare
                elif _raw_country[0] == "*":
                    _raw_country = _raw_country.replace("*", "") 
                    cmp = start_star_compare

```

After all mwms for a country had beed generated in a directory like
`maps_build/2021_01_20__18_06_38/210120`
I got their sizes (in Kb) with this command:

```bash
du maps_build/maps_build/2021_01_20__18_06_38/210120/*.mwm | sort -k2 > Norway.sizes
```

In fact, I renamed directory to some 2021_01_20__18_06_38-Norway and used command
```bash
du maps_build/*-Norway/[0-9]*/*.mwm | sort -k2 > Norway.sizes
```

#### Combining geo data with sizes data

Now I had a set of `<Country>_regions.json` and `<Country>.sizes` files
with geo- and sizes-data respectively on several large regions with subregions.
I used the `combine_data.py` script  to generate one big `7countries.csv`.

Yet another `4countries.csv` file with Germany, Austria, Belgium and Netherlands
subregions was already prepared before, it has excluded=1 flag for those
Netherland subregions which contain much water (inner waters, not ocean). Also,
there were not data for upper-lever regions, and the values of area, cities,
population and mwm_size were obtained as the sum of subregions defined by
parent_id column.

Set 'is_leaf' property in `4countries.csv`
```python
import pandas as pd
data1 = pd.read_csv('data/4countries.csv', sep=';')  # Austria, Belgium, Netherlands, Germany
data1['is_leaf'] = data1.apply(lambda row:
    1 if len(data1[data1['parent_id'] == row['id']]) == 0 else 0
    , axis=1)
data1.to_csv('data/4countries.csv', index=False, sep=';')
```

Since data for country-level regions was not collected (due to long sql queries and
mwm generation time), we enrich the `7countries.csv` dataset with country-level
by summing up data of subregions:
```python
import pandas as pd
data7 = pd.read_csv('data/7countries.csv', sep=';')

# Drop data for countries if it present
data7 = data7[data7['al'] != 2]

countries = {'id':   [-59065, -2978650, -51701, -382313, -62149],
             'name': ['Belarus', 'Norway', 'Switzerland', 'Japan', 'United Kingdom'],
             'excluded': [0]*5,
             'al': [2]*5,
        }
sum_fields = ('full_area', 'land_area', 'city_cnt', 'hamlet_cnt', 'city_pop', 'hamlet_pop', 'mwm_size_sum')

for field in sum_fields:
    field_values = [data7[data7['parent_id'] == c_id][field].sum() for c_id in countries['id']]
    countries[field] = field_values

countries_df = pd.DataFrame(countries, columns = list(countries.keys()))
data7 = pd.concat([data7, countries_df])
data7.to_csv('data/7countries-1.csv', index=False, sep=';')

# Check, and if all right, do
# import os; os.rename('data/7countries-1.csv', 'data/7countries.csv')
```

The union of `4countries.csv` and `7countries.csv` data is the
dataset for data science experiments on mwm size prediction. Keep in mind
that _mwm_size_ field may be NULL (for countries), or _mwm_size_sum_ may be NULL
(in 4countries.csv). Make corrections when getting combined dataset:

```python
import pandas as pd
import numpy as np

def fit_mwm_size(df):
    df['mwm_size'] = np.where(df['mwm_size'].isnull(), df['mwm_size_sum'], df['mwm_size'])

data1 = pd.read_csv('data/4countries.csv', sep=';')  # Austria, Belgium, Netherlands, Germany
data2 = pd.read_csv('data/7countries.csv', sep=';')  # Norway, UK, US(4 states), Switzerland, Japan, Belarus, Ile-de-France

data = pd.concat([data1, data2])

data = data[data.excluded.eq(0) & data.id.notnull()]

fit_mwm_size(data)
```
