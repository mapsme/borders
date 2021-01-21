import json
import csv


def get_combined_info(region_name):
    with open(f'data/{region_name}_regions.json', newline='') as f:
        regions = json.load(f)
        regions = {int(k):v for k, v in regions.items()}

    with open(f'data/{region_name}.sizes') as sizes_file:
        for line in sizes_file:
            mwm_name = line.split('/')[-1][:-4]
            #print(f"mwm_name = {mwm_name}")
            r_id = -int(mwm_name.split('_')[0])
            if r_id not in regions:
                raise Exception(f'id {r_id} not in {region_name} data')
            size = int(line.split()[0])
            name = mwm_name.split('_')[-1]
            country = mwm_name.split('_')[1]

            regions[r_id].update({
                'mwm_name': mwm_name,
                'country': country,
                'mwm_size': size,
            })


    admin_levels = set(x['al'] for x in regions.values())

    ids_to_remove = []  # Far oversea regions may be counted but no mwm generated for
    for al in sorted(admin_levels, reverse=True):
        for r_id, r_data in ((r_id, r_data) for r_id, r_data in regions.items() if r_data['al'] == al):
            children = [ch for ch in regions.values() if ch['parent_id'] == r_id]
            is_leaf = not bool(children)
            r_data['is_leaf'] = int(is_leaf)
            r_data['excluded'] = 0
            if is_leaf:
                if 'mwm_size' not in r_data:
                    print(f"Mwm not generated for {r_data['name']}")
                    ids_to_remove.append(r_id)
                else:
                    r_data['mwm_size_sum'] = r_data['mwm_size']
            else:
                r_data['mwm_size_sum'] = sum(ch['mwm_size'] for ch in children)

    return {k:v for k,v in regions.items() if k not in ids_to_remove}


def main():
    region_names = [
        'Belarus', 'Switzerland', 'Ile-de-France',
        'United Kingdom', 'Norway', 'Japan', 'United States'
    ]

    rows = []

    # full_area includes ocean.
    fieldnames = ['id', 'parent_id', 'al', 'is_leaf', 'excluded', 'name', 'mwm_name', 'country',
                  'city_cnt', 'city_pop', 'hamlet_cnt', 'hamlet_pop',
                  'full_area', 'land_area', 'mwm_size', 'mwm_size_sum']

    with open('data/7countries.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=fieldnames)
        writer.writeheader()

        for region_name in region_names:
            regions = get_combined_info(region_name)
            rows = sorted(regions.values(), key=lambda reg: (reg['al'], reg['name']))
            writer.writerows(rows)


if __name__ == '__main__':
    main()
