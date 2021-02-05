# MWM size prediction model

The web application uses a data-science-based model to predict the size of MWM file
compiled on some area. Here described are the efforts that were undertaken
to build such a prediction model. The serialized model resides at `web/app/data/`
in the `model.pkl` and `scaler.pkl` files. Its first variant was trained only
on county-level data and is valid at limited parameters range (see `web/app/config.py`
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
which is too small for MWMs while regions of one level higher are too big,
so that a usual MWM would be a cluster of "counties".

Japan was a special case &ndash; leaf regions are mix of (6) and (7)-al subregions,
so `extract_mwm_geo_data.py` script contains a
function to split the country into subregions.

The `extract_mwm_geo_data.py` script, endowed with a valid connection
to the database with borders, gathers information about all borders of a
given country/region and its descendants: id, parent_id, admin level, name,
full area, land area (so the table with land borders of the planet is necessary),
city/town count and population, hamlet/village count and population, and from
some timepoint &ndash; coastline length.

One should keep in mind that some borders may be absent in OSM, so a region may
not be fully covered with subregions. So, a region area (or places count,
or population) may be greater than the sum of areas of its detected subregions.
One way is to fix borders by hand. Another way, that I followed, is to select
areas, cities and population from the database even for upper-level regions
(except countries, for which the calculation would run too long and is not very useful).

#### Mwm size data gathering

Having borders division of the training countries in the web app, I download all
borders, changing the poly-file naming procedure so that the name to contain
the region id. The id would be the link between files with geodata and mwm sizes data.
So we have many border files with names like _03565917_Japan_Gunma Prefecture_Numata.poly_
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

Its important to be aware that an mwm would include data to the other side of
its border if there is no generated mwm to the other side of the border. So,
if the country extract does not strictly follow its border, a gingle near-border mwms could be
overestimated by 10-30%, the sum of all country mwms - up to 1.5%. Either use precise country.poly file
or cheet with CountriesFilesIndexAffilation in 2 places of generator sources:

```bash
diff --git a/generator/final_processor_country.cpp b/generator/final_processor_country.cpp
index b4aaa8dbbd..141ae88665 100644
--- a/generator/final_processor_country.cpp
+++ b/generator/final_processor_country.cpp
@@ -45,7 +45,7 @@ CountryFinalProcessor::CountryFinalProcessor(std::string const & borderPath,
   , m_temporaryMwmPath(temporaryMwmPath)
   , m_intermediateDir(intermediateDir)
   , m_affiliations(
-        std::make_unique<CountriesFilesIndexAffiliation>(m_borderPath, haveBordersForWholeWorld))
+        std::make_unique<CountriesFilesAffiliation>(m_borderPath, haveBordersForWholeWorld))
   , m_threadsCount(threadsCount)
 {
 }
diff --git a/generator/processor_country.cpp b/generator/processor_country.cpp
index a122cbc926..e079cbb643 100644
--- a/generator/processor_country.cpp
+++ b/generator/processor_country.cpp
@@ -20,7 +20,7 @@ ProcessorCountry::ProcessorCountry(std::shared_ptr<FeatureProcessorQueue> const
   m_processingChain = std::make_shared<RepresentationLayer>(m_complexFeaturesMixer);
   m_processingChain->Add(std::make_shared<PrepareFeatureLayer>());
   m_processingChain->Add(std::make_shared<CountryLayer>());
-  auto affiliation = std::make_shared<feature::CountriesFilesIndexAffiliation>(
+  auto affiliation = std::make_shared<feature::CountriesFilesAffiliation>(
       bordersPath, haveBordersForWholeWorld);
   m_affiliationsLayer =
       std::make_shared<AffiliationsFeatureLayer<>>(kAffiliationsBufferSize, affiliation, m_queue); 
```
Also, do not use ~ in -B option of osmconvert: "-B=~/borders/Austria.poly" would not expand tilde
into $HOME in the middle of the word which leads into strange "no polygon file or too large" error.

In `maps_generation.ini` I changed the path to the planet and md5sum file and run
the MWMs generation with
```bash
nohup python -m maps_generator --order="" --skip="Routing,RoutingTransit" \
    --countries="*_Switzerland_*" &
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

In fact, I renamed directories to somewhat 2021_01_20__18_06_38-Norway
and used commands like
```bash
du maps_build/*-Norway/[0-9]*/*.mwm | sort -k2 > Norway.sizes
```

#### Combining geo data with sizes data

Now I had a set of `<Country>_regions.json` and `<Country>.sizes` files
with geo- and sizes-data respectively on several large regions with subregions.
I used the `combine_data.py` script  to generate one big `countries.csv`.

Some regions of Netherlands which contain much water (inner waters, not ocean)
have excluded=1 flag. The list of excluded 8-level regions is hardcoded, and the `size`
column for parents that contain them is calculate as the sum of not excluded children.


The `countries.csv` data is the
dataset for data science experiments on mwm size prediction with _size_ column as
target value:

```python
import pandas as pd

data = pd.read_csv('data/countries.csv', sep=';')
data = data[data.excluded.eq(0) & data.id.notnull()]
```

#### New dataset parameters

Leaves, roughly speaking, area counties. Not leaves are states/prefectures/provinces.
Country-sized regions were not included into the dataset.

```python
# Mean size of a leaf region (county/city)
data[data['is_leaf'] == 1]['mwm_size'].mean()
2985.1346153846152

# Mean size of non-leaf regions
data[data['is_leaf'] == 0]['mwm_size'].mean()
77270.27027027027

# Overall mean
data['mwm_size'].mean()
6349.332925336597

```


#### Results of training of the model

On the extended dataset base on 11 countries the best result for the model
tuning is neg_mean_square_error = -2940346333.0175967 at best_params: 
{'C': 100000, 'epsilon': 5, 'gamma': 'auto', 'kernel': 'rbf'} (use `my_grid_search()`
function in `main()` of the `data_science.py` script)
which is worse than on initial 4 countries.
Those 4 countries have little coastline except the Netherlands, and Netherlands regions
were usually underestimated. So the way of improvement is to take coastline factor
length into account. 


### Coastline inclusion

MWMs were generated with and without coastline for some regions with highly rugged
coastline. omim branch was master at commit near 6ef8146fb19bb3cf5bfaaf3c994d5a010031730c (12 Jan 2021).
The bash command used for the generation were like:

```bash
nohup python -m maps_generator --order="" --skip="Routing,RoutingTransit,Coastline" \
 --countries="*_Scotland_Highland*,*_Scotland_Western Isles"
```
The difference was `Coastline` in `--skip` option and, naturally, `--countries`: \
"\*_Scotland_Highland\*,\*_Scotland_Western Isles" \
"\*_Nordland_Steigen,\*_Nordland_Meloy,\*_Nordland" \
"\*_Amakusa,\*_Goto"


The MWM sizes are in bytes:

|                                       |with coast| no coast |coast %|
| ------------------------------------- | --------:| --------:| -----:|
|United Kingdom_Scotland_Highland	    | 25809723 | 24557284 |  4,85 |
|United Kingdom_Scotland_Western Isles	|    57879 | 20759    | 64,13 |
|Norway_Nordland_Steigen                |  7179127 | 6109432  | 14,90 |
|Norway_Nordland_Meloy                  |  4564915 | 3864559  | 15,34 |
|Norway_Nordland                        | 108832710| 100420238|  7,73 |
|Japan_Kumamoto Prefecture_Amakusa      |  4221092 | 3756787  | 11,00 |
|Japan_Nagasaki Prefecture_Goto         |  1002103 | 690700   | 31,08 |

We make conclusion that the length of coastline is worth to be taken into account.


#### Load coastlines into the database

```bash
wget https://osmdata.openstreetmap.de/download/coastlines-split-4326.zip
unzip coastlines-split-4326.zip
cd coastlines-split-4326/
shp2pgsql -s 4326 lines.shp coastlines | psql -d az_gis3
CREATE INDEX coastlines_geom_idx ON coastlines USING GIST (geom);
```

Long coastlines are already split into smaller pieces.

`extract_mwm_data.py` script now also calculates coastline length for the regions.