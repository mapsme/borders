import json

import config


XML_ATTR_ESCAPINGS = {
    '&': '&amp;',
    '>': '&gt;',
    '<': '&lt;',
    '\n': '&#10;',
    '\r': '&#13;',
    '\t': '&#9;',
    '"': '&quot;'
}


def _quoteattr(value):
    for char, replacement in XML_ATTR_ESCAPINGS.items():
        value = value.replace(char, replacement)
    return f'"{value}"'


def get_xml_header():
    return ('<?xml version="1.0" encoding="UTF-8"?>'
            '<osm version="0.6" upload="false">')


def _ring_hash(refs):
    #return json.dumps(refs)
    return hash(tuple(sorted(refs)))


def _parse_polygon(node_pool, rings, polygon):
    role = 'outer'
    for ring in polygon:
        rings.append([role, _parse_linestring(node_pool, ring)])
        role = 'inner'


def _parse_linestring(node_pool, linestring):
    nodes = []
    for lonlat in linestring:
        ref = f'{lonlat[1]} {lonlat[0]}'
        if ref in node_pool:
            node_id = node_pool[ref]
        else:
            node_id = node_pool['id']
            node_pool[ref] = node_id
            node_pool['id'] = node_id + 1
        nodes.append(node_id)
    return nodes


def _append_way(way, way2):
    another = list(way2) # make copy to not modify original list
    if way[0] == way[-1] or another[0] == another[-1]:
        return None
    if way[0] == another[0] or way[-1] == another[-1]:
        another.reverse()
    if way[-1] == another[0]:
        result = list(way)
        result.extend(another[1:])
        return result
    elif way[0] == another[-1]:
        result = another
        result.extend(way)
        return result
    return None


def _way_to_wkt(node_pool, refs):
    coords_sequence = (f"{node_pool[nd]['lon']} {node_pool[nd]['lat']}"
                        for nd in refs)
    return f"({','.join(coords_sequence)})"


def borders_to_xml(borders):
    node_pool = {'id': 1}  # 'lat_lon': id
    regions = []  # { id: id, name: name, rings: [['outer', [ids]], ['inner', [ids]], ...] }
    for border in borders:
        geometry = border['geometry']
        rings = []
        if geometry['type'] == 'Polygon':
            _parse_polygon(node_pool, rings, geometry['coordinates'])
        elif geometry['type'] == 'MultiPolygon':
            for polygon in geometry['coordinates']:
                _parse_polygon(node_pool, rings, polygon)
        if len(rings) > 0:
            regions.append({
                'id': abs(border['properties']['id']),
                'name': border['properties']['name'],
                'disabled': border['properties']['disabled'],
                'rings': rings
            })

    xml = get_xml_header()

    for latlon, node_id in node_pool.items():
        if latlon != 'id':
            (lat, lon) = latlon.split()
            xml += (f'<node id="{node_id}" visible="true" version="1" '
                    f'lat="{lat}" lon="{lon}" />')

    ways = {}  # _ring_hash => id
    wrid = 1
    for region in regions:
        w1key = _ring_hash(region['rings'][0][1])
        if (not config.JOSM_FORCE_MULTI and
                len(region['rings']) == 1 and
                w1key not in ways
        ):
            # simple case: a way
            ways[w1key] = region['id']
            xml += f'''<way id="{region['id']}" visible="true" version="1">'''
            xml += f'''<tag k="name" v={region['name']} />'''
            if region['disabled']:
                xml += '<tag k="disabled" v="yes" />'
            for nd in region['rings'][0][1]:
                xml += f'<nd ref="{nd}" />'
            xml += '</way>'
        else:
            # multipolygon
            rxml = f'''<relation id="{region['id']}" visible="true" version="1">'''
            wrid += 1
            rxml += '<tag k="type" v="multipolygon" />'
            rxml += f'''<tag k="name" v={_quoteattr(region['name'])} />'''
            if region['disabled']:
                rxml += '<tag k="disabled" v="yes" />'
            for ring in region['rings']:
                wkey = _ring_hash(ring[1])
                if wkey in ways:
                    # already have that way
                    rxml += f'<member type="way" ref="{ways[wkey]}" role="{ring[0]}" />'
                else:
                    ways[wkey] = wrid
                    xml += f'<way id="{wrid}" visible="true" version="1">'
                    rxml += f'<member type="way" ref="{wrid}" role="{ring[0]}" />'
                    for nd in ring[1]:
                        xml += f'<nd ref="{nd}" />'
                    xml += '</way>'
                    wrid += 1
            xml += rxml + '</relation>'
    xml += '</osm>'
    return xml


def _extend_bbox(bbox, *args):
    """Extend bbox to include another bbox or point."""
    assert len(args) in (1, 2)
    if len(args) == 1:
        another_bbox = args[0]
    else:
        another_bbox = [args[0], args[1], args[0], args[1]]
    bbox[0] = min(bbox[0], another_bbox[0])
    bbox[1] = min(bbox[1], another_bbox[1])
    bbox[2] = max(bbox[2], another_bbox[2])
    bbox[3] = max(bbox[3], another_bbox[3])


def _bbox_contains(outer, inner):
    return (outer[0] <= inner[0] and
            outer[1] <= inner[1] and
            outer[2] >= inner[2] and
            outer[3] >= inner[3])


def borders_from_xml(doc_tree):
    """Returns regions dict or str with error message."""
    root = doc_tree.getroot()

    # read nodes and ways
    nodes = {}  # id: { lat, lon, modified }
    for node in root.iter('node'):
        if node.get('action') == 'delete':
            continue
        modified = int(node.get('id')) < 0 or node.get('action') == 'modify'
        nodes[node.get('id')] = {'lat': float(node.get('lat')),
                                 'lon': float(node.get('lon')),
                                 'modified': modified }
    ways = {}  # id: { name, disabled, modified, bbox, nodes, used }
    for way in root.iter('way'):
        if way.get('action') == 'delete':
            continue
        way_nodes = []
        bbox = [1e4, 1e4, -1e4, -1e4]
        modified = int(way.get('id')) < 0 or way.get('action') == 'modify'
        for node in way.iter('nd'):
            ref = node.get('ref')
            if not ref in nodes:
                return f"Missing node {ref} in way {way.get('id')}"
            way_nodes.append(ref)
            if nodes[ref]['modified']:
                modified = True
            _extend_bbox(bbox, float(nodes[ref]['lon']), float(nodes[ref]['lat']))
        name = None
        disabled = False
        for tag in way.iter('tag'):
            if tag.get('k') == 'name':
                name = tag.get('v')
            if tag.get('k') == 'disabled' and tag.get('v') == 'yes':
                disabled = True
        if len(way_nodes) < 2:
            return f"Way with less than 2 nodes: {way.get('id')}"
        ways[way.get('id')] = {'name': name, 'disabled': disabled,
                               'modified': modified, 'bbox': bbox,
                               'nodes': way_nodes, 'used': False}

    # finally we are constructing regions: first, from multipolygons
    regions = {}  # id: { modified, disabled, wkt, type: 'r'|'w' }
    for rel in root.iter('relation'):
        if rel.get('action') == 'delete':
            continue
        osm_id = int(rel.get('id'))
        modified = osm_id < 0 or rel.get('action') == 'modify'
        name = None
        disabled = False
        multi = False
        inner = []
        outer = []
        for tag in rel.iter('tag'):
            if tag.get('k') == 'name':
                name = tag.get('v')
            if tag.get('k') == 'disabled' and tag.get('v') == 'yes':
                disabled = True
            if tag.get('k') == 'type' and tag.get('v') == 'multipolygon':
                multi = True
        if not multi:
            return f"Found non-multipolygon relation: {rel.get('id')}"
        for member in rel.iter('member'):
            ref = member.get('ref')
            if not ref in ways:
                return f"Missing way {ref} in relation {rel.get('id')}"
            if ways[ref]['modified']:
                modified = True
            role = member.get('role')
            if role == 'outer':
                outer.append(ways[ref])
            elif role == 'inner':
                inner.append(ways[ref])
            else:
                return f"Unknown role {role} in relation {rel.get('id')}"
            ways[ref]['used'] = True
        # after parsing ways, so 'used' flag is set
        if rel.get('action') == 'delete':
            continue
        if len(outer) == 0:
            return f"Relation {rel.get('id')} has no outer ways"
        # reconstruct rings in multipolygon
        for multi in (inner, outer):
            i = 0
            while i < len(multi):
                way = multi[i]['nodes']
                while way[0] != way[-1]:
                    productive = False
                    j = i + 1
                    while way[0] != way[-1] and j < len(multi):
                        new_way = _append_way(way, multi[j]['nodes'])
                        if new_way:
                            multi[i] = dict(multi[i])
                            multi[i]['nodes'] = new_way
                            way = new_way
                            if multi[j]['modified']:
                                multi[i]['modified'] = True
                            _extend_bbox(multi[i]['bbox'], multi[j]['bbox'])
                            del multi[j]
                            productive = True
                        else:
                            j += 1
                    if not productive:
                        return f"Unconnected way in relation {rel.get('id')}"
                i += 1
        # check for 2-node rings
        for multi in (outer, inner):
            for way in multi:
                if len(way['nodes']) < 3:
                    return f"Way in relation {rel.get('id')} has only {len(way['nodes'])} nodes"
        # sort inner and outer rings
        polygons = []
        for way in outer:
            rings = [_way_to_wkt(nodes, way['nodes'])]
            for i in range(len(inner)-1, -1, -1):
                if _bbox_contains(way['bbox'], inner[i]['bbox']):
                    rings.append(_way_to_wkt(nodes, inner[i]['nodes']))
                    del inner[i]
            polygons.append('({})'.format(','.join(rings)))
        regions[osm_id] = {
                'id': osm_id,
                'type': 'r',
                'name': name,
                'modified': modified,
                'disabled': disabled,
                'wkt': 'MULTIPOLYGON({})'.format(','.join(polygons))
        }

    # make regions from unused named ways
    for wid, w in ways.items():
        if w['used']:
            continue
        if not w['name']:
            #continue
            return f"Unused in multipolygon way with no name: {wid}"
        if w['nodes'][0] != w['nodes'][-1]:
            return f"Non-closed unused in multipolygon way: {wid}"
        if len(w['nodes']) < 3:
            return f"Way {wid} has {len(w['nodes'])} nodes"
        regions[wid] = {
                'id': int(wid),
                'type': 'w',
                'name': w['name'],
                'modified': w['modified'],
                'disabled': w['disabled'],
                'wkt': 'POLYGON({})'.format(_way_to_wkt(nodes, w['nodes']))
        }

    return regions


def lines_to_xml(lines_geojson_iterable):
    node_pool = {'id': 1}  # 'lat_lon': id
    lines = []
    for feature in lines_geojson_iterable:
        geometry = json.loads(feature)
        if geometry['type'] == 'LineString':
            nodes = _parse_linestring(node_pool, geometry['coordinates'])
        elif geometry['type'] == 'MultiLineString':
            nodes = []
            for line in geometry['coordinates']:
                nodes.extend(_parse_linestring(node_pool, line))
        if len(nodes) > 0:
            lines.append(nodes)

    xml = get_xml_header()

    for latlon, node_id in node_pool.items():
        if latlon != 'id':
            (lat, lon) = latlon.split()
            xml += (f'<node id="{node_id}" visible="true" version="1" '
                    f'lat="{lat}" lon="{lon}" />')
    wrid = 1
    for line in lines:
        xml += f'<way id="{wrid}" visible="true" version="1">'
        for nd in line:
            xml += f'<nd ref="{nd}" />'
        xml += '</way>'
        wrid += 1
    xml += '</osm>'
    return xml
