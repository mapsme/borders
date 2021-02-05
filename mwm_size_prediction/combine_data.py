import json
import csv


excluded_region_ids = set([
    # Netherland regions that contain much inner waters
    -47531, -47609, -47823, -161150, -289288, -291667, -296462, -296469,
    -296471, -296493, -296984, -297066, -307745, -319055, -384538, -408115,
    -409764, -409806, -412269, -412348, -1357943, -1946377, -3410689, -5816820, -9172188,

    # City of Bristol (-57539) has exaggerated coastline length. Exclude or use correct value!
])


def get_combined_info(region_name):
    with open(f'data/{region_name}_regions.json', newline='') as f:
        regions = json.load(f)
        regions = {int(k): v for k, v in regions.items()}

    with open(f'data/{region_name}.sizes') as sizes_file:
        for line in sizes_file:
            mwm_name = line.strip().split('/')[-1][:-4]
            #print(f"mwm_name = {mwm_name}")
            r_id = -int(mwm_name.split('_')[0])
            if r_id not in regions:
                raise Exception(f'id {r_id} not in {region_name} data')
            size = int(line.split()[0])
            name = mwm_name.split('_')[-1]
            country = mwm_name.split('_')[1]

            regions[r_id].update({
                'mwm_name': mwm_name,
                'name': name,
                'country': country,
                'mwm_size': size,
                'size': None  # fair size for prediction = size of generated mwm_size, or sum of not excluded children mwms
            })


    admin_levels = set(x['al'] for x in regions.values())

    has_excluded_children_region_ids = set()

    ids_to_remove = []  # Far oversea regions, or very large regions (Netherlands(3)) may be counted but no mwm generated for
    for al in sorted(admin_levels, reverse=True):
        for r_id, r_data in ((r_id, r_data) for r_id, r_data in regions.items() if r_data['al'] == al):
            if 'mwm_size' not in r_data:
                print(f"Mwm not generated for {r_data['name']}")
                ids_to_remove.append(r_id)
                continue

            children = [ch for ch in regions.values() if ch['parent_id'] == r_id]
            is_leaf = not bool(children)
            r_data['is_leaf'] = int(is_leaf)

            r_data['excluded'] = int(r_id in excluded_region_ids)

            if r_id in excluded_region_ids or r_id in has_excluded_children_region_ids:
                parent_id = r_data['parent_id']
                if parent_id is not None:
                    has_excluded_children_region_ids.add(parent_id)

            if is_leaf:
                r_data['size'] = r_data['mwm_size']
            else:
                has_defective_children = sum(
                    1 for ch in children
                    if ch['id'] in excluded_region_ids
                        or ch['id'] in has_excluded_children_region_ids
                ) > 0
                if not has_defective_children:
                    r_data['size'] = r_data['mwm_size']
                else:
                    for f in ('city_cnt', 'city_pop', 'hamlet_cnt', 'hamlet_pop',
                              'full_area', 'land_area', 'coastline_length',
                              'size'):
                        r_data[f] = sum(ch[f] for ch in children
                                       if ch['id'] not in excluded_region_ids)

    if has_excluded_children_region_ids:
        from pprint import pprint as pp
        pp([(regions[k]['id'], regions[k]['name'], regions[k]['al'])
            for k in has_excluded_children_region_ids])

    return {k: v for k, v in regions.items() if k not in ids_to_remove}


def main():
    region_names = [
        'Belarus', 'Switzerland', 'Ile-de-France',
        'United Kingdom', 'Norway', 'Japan', 'United States',

        'Germany', 'Austria', 'Belgium', 'Netherlands'
    ]

    # full_area includes ocean.
    fieldnames = ['id', 'parent_id', 'al', 'is_leaf', 'excluded', 'name', 'mwm_name', 'country',
                  'city_cnt', 'city_pop', 'hamlet_cnt', 'hamlet_pop',
                  'full_area', 'land_area', 'coastline_length',
                  'mwm_size', 'size']

    with open('data/countries.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=fieldnames)
        writer.writeheader()

        for region_name in region_names:
            regions = get_combined_info(region_name)
            rows = sorted(regions.values(), key=lambda reg: (reg['al'], reg['name']))
            writer.writerows(rows)


if __name__ == '__main__':
    main()
