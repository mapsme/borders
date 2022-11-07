var STYLE_BORDER = {
    stroke: true,
    color: '#03f',
    weight: 3,
    fill: true,
    fillOpacity: 0.1
};
var STYLE_SELECTED = {
    stroke: true,
    color: '#ff3',
    weight: 3,
    fill: true,
    fillOpacity: 0.75
};
var STYLE_OLD = {
    color: 'purple',
    weight: 5,
    fill: false,
    clickable: false
};
var STYLE_PARENT = {
    color: 'black',
    weight: 6,
    fill: false
};

var FILL_TOO_SMALL = '#0f0';
var FILL_TOO_BIG = '#800';
var FILL_ZERO = 'black';

var map, borders = {},
    bordersLayer, selectedId, editing = false;
var config = { // server config
    READONLY: false,
    MWM_SIZE_THR: 70,
    OLD_BORDERS_NAME: undefined // may be filled in getServerConfiguration()
};
var size_good, size_bad;
var maxRank = 1;
var tooSmallLayer = null;
var oldBordersLayer = null;

function init() {
    map = L.map('map', {
        editable: true
    }).setView([30, 0], 3);
    var hash = new L.Hash(map);
    L.tileLayer('https://tile.openstreetmap.de/{z}/{x}/{y}.png', {
        attribution: '&copy; OpenStreetMap'
    }).addTo(map);
    //L.tileLayer('https://b.tile.openstreetmap.fr/osmfr/{z}/{x}/{y}.png', { attribution: '&copy; OpenStreetMap' }).addTo(map);
    //L.tileLayer('http://tile.openstreetmap.org/{z}/{x}/{y}.png', { attribution: '&copy; OpenStreetMap' }).addTo(map);
    bordersLayer = L.layerGroup();
    map.addLayer(bordersLayer);

    map.on('moveend', function() {
        if (map.getZoom() >= 5)
            updateBorders();
        $('#b_josm').css('visibility', map.getZoom() >= 7 ? 'visible' :
            'hidden');
    });

    $('#poly_all').attr('href', getPolyDownloadLink());
    $('#poly_bbox').on('mousedown', function() {
        $(this).attr('href', getPolyDownloadLink(true));
    });
    $('#r_green').val(size_good);
    $('#r_red').val(size_bad);
    $('#hide_import_button').click(function() {
        $('#import_div').hide();
        $('#filefm input[type=file]').val('');
    });
    $('#h_iframe').load(function() {
        console.log('frame loaded');
        var frame_doc = $('#h_iframe')[0].contentWindow.document;
        var frame_body = $('body', frame_doc);
        frame_body.css({
            'font-size': '9pt'
        });
        updateBorders();
    });
    $('#fsearch').keyup(function(e) {
        if (e.keyCode == 13)
            $('#b_search').click();
    });
    $('#b_comment').keyup(function(e) {
        if (e.keyCode == 13)
            $('#b_comment_send').click();
    });
    $('#auto_divide').change(function() {
        if (this.checked)
            $('#population_thresholds').show();
        else
            $('#population_thresholds').hide();
    });
    getServerConfiguration();
    filterSelect(true);
}

function getServerConfiguration() {
    $.ajax(getServer('config'), {
        success: function(res) {
            if (res.osm) {
                $('#unbound_actions').show();
                $('#b_divide').show();
            }
            if (res.tables && res.tables.length > 0) {
                config.OLD_BORDERS_NAME = res.tables[0];
                $('#old_action').show();
                $('#josm_old').css('display', 'inline');
            }
            if (res.backup) {
                $('#backups').show();
            }
            if (res.readonly) {
                $('#action_buttons').hide();
                $('#backups').hide();
                config.READONLY = true;
            }
            if (!res.readonly && IMPORT_ENABLED) {
                $('#filefm').css('display', 'block');
                $('#filefm').attr('action', getServer('import'));
                var iframe =
                    '<iframe name="import_frame" class="h_iframe" src="about:blank"></iframe>';
            }
            size_bad = config.MWM_SIZE_THR = Math.round(parseInt(res
                .mwm_size_thr) / 1024);
            size_good = Math.round(size_bad * 0.7 / 10) * 10;
            $('#r_green').val(size_good);
            $('#r_red').val(size_bad);
            $('#mwm_size_thr').val(config.MWM_SIZE_THR);
        }
    });
}

function updateBorders() {
    var b = map.getBounds(),
        simplified = map.getZoom() < 7 ? 2 : (map.getZoom() < 11 ? 1 : 0);
    $.ajax(getServer('bbox'), {
        data: {
            'simplify': simplified,
            'xmin': b.getWest(),
            'xmax': b.getEast(),
            'ymin': b.getSouth(),
            'ymax': b.getNorth()
        },
        success: makeAnswerHandler(processBorders),
        dataType: 'json',
        simplified: simplified
    });

    if (oldBordersLayer != null && config.OLD_BORDERS_NAME) {
        oldBordersLayer.clearLayers();
        $.ajax(getServer('bbox'), {
            data: {
                'table': config.OLD_BORDERS_NAME,
                'simplify': simplified,
                'xmin': b.getWest(),
                'xmax': b.getEast(),
                'ymin': b.getSouth(),
                'ymax': b.getNorth()
            },
            success: makeAnswerHandler(processOldBorders),
            dataType: 'json'
        });
    }
}

function makeAnswerHandler(on_ok_func) {
    return function(answer) {
        if (answer.status !== 'ok')
            alert(answer.status);
        else {
            if (answer.warnings && answer.warnings.length)
                alert(answer.warnings.join('\n'));
            on_ok_func(answer);
        }
    };
}

function processBorders(data) {
    data = data.geojson;
    for (var id in borders) {
        if (id != selectedId || !editing) {
            bordersLayer.removeLayer(borders[id].layer);
            delete borders[id];
        }
    }

    for (var f = 0; f < data.features.length; f++) {
        var layer = L.GeoJSON.geometryToLayer(data.features[f].geometry),
            props = data.features[f].properties;
        props.simplified = this.simplified;
        updateBorder(props.id, layer, props);
    }
    if (selectedId in borders) {
        selectLayer({
            target: borders[selectedId].layer
        });
    }
    else {
        selectLayer(null);
    }

    [subregionsLayer, clustersLayer,
        parentLayer, potentialParentLayer
    ].forEach(
        function(layer) {
            if (layer)
                layer.bringToFront();
        }
    );

    var b = map.getBounds();
    if (tooSmallLayer != null) {
        tooSmallLayer.clearLayers();
        $.ajax(getServer('small'), {
            data: {
                'xmin': b.getWest(),
                'xmax': b.getEast(),
                'ymin': b.getSouth(),
                'ymax': b.getNorth()
            },
            success: processTooSmall,
            dataType: 'json'
        });
    }
}

function processOldBorders(data) {
    var layer = L.geoJson(data.geojson, {
        style: STYLE_OLD
    });
    oldBordersLayer.addLayer(layer);
}

function processTooSmall(data) {
    if (tooSmallLayer == null || !data || !('rings' in data))
        return;
    tooSmallLayer.clearLayers();
    for (var i = 0; i < data.rings.length; i++) {
        var point = data.rings[i];
        if (point.id in borders) {
            var marker = L.marker(
                [point.lat, point.lon], {
                    title: point.name + '\n' + 'Площадь: ' +
                        L.Util.formatNum(point.area, 2) + ' км²'
                }
            );
            marker.pLayer = borders[point.id].layer;
            marker.on('click', selectLayer);
            tooSmallLayer.addLayer(marker);
        }
    }
}

function updateBorder(id, layer, props) {
    if (id in borders) {
        if (id == selectedId && editing)
            return;
        bordersLayer.removeLayer(borders[id].layer);
    }
    borders[id] = props;
    borders[id].layer = layer;
    layer.id = id;
    bordersLayer.addLayer(layer);
    layer.setStyle(STYLE_BORDER);
    if (borders[id]['disabled'])
        layer.setStyle({
            fillOpacity: 0.01
        });
    var color = getColor(borders[id]);
    layer.setStyle({
        color: color
    });
    layer.defStyle = color;
    layer.on('click', selectLayer);
}

function selectLayer(e) {
    if (e != null && 'pLayer' in e.target)
        e.target = e.target.pLayer;

    if (e != null && joinSelected != null) {
        bJoinSelect(e.target);
        return;
    }
    if (selectedId && selectedId in borders) {
        borders[selectedId].layer.setStyle(STYLE_BORDER);
        if (borders[selectedId]['disabled'])
            borders[selectedId].layer.setStyle({
                fillOpacity: 0.01
            });
        if ('defStyle' in borders[selectedId].layer)
            borders[selectedId].layer.setStyle({
                color: borders[selectedId].layer.defStyle
            });
    }
    if (e != null && 'id' in e.target && e.target.id in borders) {
        selectedId = e.target.id;
        if (selectedIdForParentAssigning &&
            selectedIdForParentAssigning != selectedId) {
            finishChooseParent();
        }
        e.target.setStyle(STYLE_SELECTED);
        var props = borders[selectedId];
        if (props['disabled'])
            e.target.setStyle({
                fillOpacity: 0.01
            });
        $('#b_name').text(props['name']);
        $('#b_al').text(
            props['admin_level'] ? '(' + props['admin_level'] + ')' : ''
        );
        $('#b_parent_name').text(getParentBorderRepresentation(props));
        $('#b_size').text(
            Math.round(props['count_k'] * BYTES_FOR_NODE / 1024 / 1024) + ' MB'
        );
        var mwm_size_est = props['mwm_size_est'];
        var mwm_size_est_text = mwm_size_est === null ? '-' : Math.round(props['mwm_size_est']/1024) + ' MB';
        $('#pa_size').text(mwm_size_est_text);
        $('#b_nodes').text(props['nodes']);
        $('#b_date').text(props['modified']);
        $('#b_area').text(L.Util.formatNum(props['area'] / 1000000, 2));
        $('#b_comment').val(props['comment'] || '');
        //$('#b_status').text(props['disabled'] ? 'Отключено' : 'В сборке');
        $('#b_disable').text(props['disabled'] ? 'Вернуть' : 'Убрать');
    }
    else
        selectedId = null;

    if (selectedId)
        $('#selected_border_actions').show();
    else
        $('#selected_border_actions').hide();

    $('#rename').hide();
}

function filterSelect(noRefresh) {
    var value = $('#f_type').val();
    $('#f_size').css('display', value.endsWith('size') ? 'block' : 'none');
    $('#f_chars').css('display', value == 'chars' ? 'block' : 'none');
    $('#f_comments').css('display', value == 'comments' ? 'block' : 'none');
    $('#f_topo').css('display', value == 'topo' ? 'block' : 'none');
    if (value == 'topo') {
        tooSmallLayer = L.layerGroup();
        map.addLayer(tooSmallLayer);
    }
    else if (tooSmallLayer != null) {
        map.removeLayer(tooSmallLayer);
        tooSmallLayer = null;
    }
    if (!noRefresh)
        updateBorders();
}

var colors = ['red', 'orange', 'yellow', 'lime', 'green', 'olive', 'cyan',
    'darkcyan', 'blue', 'navy', 'magenta', 'purple', 'deeppink', 'brown'
] //'black';
var alphabet = 'abcdefghijklmnopqrstuvwxyz';

function getStringHash(str) {
    var hash = 0,
        i, chr;
    /*
    for (i = 0; i < str.length; i++) {
        chr   = str.charCodeAt(i);
        hash  = ((hash << 5) - hash) + chr;
        hash |= 0; // Convert to 32bit integer
    }
    */
    hash = str.charCodeAt(0) + str.charCodeAt(1);
    return hash;
}

function getCountryColor(props) {
    var country_name = props.country_name;
    if (!country_name)
        return 'black';
    var hash = getStringHash(country_name);
    var indexInColors = Math.abs(hash) % colors.length;
    return colors[indexInColors];
}

function getColor(props) {
    var color = STYLE_BORDER.color;
    var fType = $('#f_type').val();
    if (fType == 'nodes_size') {
        if (props['count_k'] <= 0)
            color = FILL_ZERO;
        else if (props['count_k'] * BYTES_FOR_NODE < size_good * 1024 * 1024)
            color = FILL_TOO_SMALL;
        else if (props['count_k'] * BYTES_FOR_NODE > size_bad * 1024 * 1024)
            color = FILL_TOO_BIG;
    }
    else if (fType == 'predict_size') {
        if (props['mwm_size_est'] <= 0)
            color = FILL_ZERO;
        else if (props['mwm_size_est'] < size_good * 1024)
            color = FILL_TOO_SMALL;
        else if (props['mwm_size_est'] > size_bad * 1024)
            color = FILL_TOO_BIG;
    }
    else if (fType == 'topo') {
        var rings = countRings([0, 0], props.layer);
        if (rings[1] > 0)
            color = FILL_TOO_BIG;
        else if (rings[0] == 1)
            color = FILL_TOO_SMALL;
        else if (rings[0] == 0)
            color = FILL_ZERO;
    }
    else if (fType == 'chars') {
        if (!/^[\x20-\x7F]*$/.test(props['name']))
            color = FILL_TOO_BIG;
        else if (props['name'].indexOf(' ') < 0)
            color = FILL_TOO_SMALL;
    }
    else if (fType == 'comments') {
        if (props['comment'] && props['comment'] != '')
            color = FILL_TOO_BIG;
    }
    else if (fType == 'country') {
        color = getCountryColor(props);
    }
    return color;
}

function countRings(rings, polygon) {
    if (polygon instanceof L.MultiPolygon) {
        polygon.eachLayer(function(layer) {
            rings = countRings(rings, layer);
        });
    }
    else if (polygon instanceof L.Polygon) {
        rings[0]++;
        if ('_holes' in polygon && 'length' in polygon._holes)
            rings[1] += polygon._holes.length;
    }
    return rings;
}

function doSearch() {
    var query = $('#fsearch').val();
    if (query.length > 1) {
        $.ajax(getServer('search'), {
            data: {
                'q': query
            },
            success: makeAnswerHandler(zoomToFound)
        });
    }
}

function zoomToFound(result) {
    $('#fsearch').val('');
    if (!('bounds' in result))
        return;
    var b = result['bounds'];
    if (b.length != 4)
        return;
    map.fitBounds([[b[1], b[0]], [b[3], b[2]]]);
}

function bUpdateColors() {
    size_good = +$('#r_green').val();
    if (size_good <= 0)
        size_good = 10;
    size_bad = +$('#r_red').val();
    if (size_bad <= size_good)
        size_bad = size_good * 10;
    $('#r_green').val(size_good);
    $('#r_red').val(size_bad);
    updateBorders();
}

function bOldBorders() {
    if ($('#old').prop('checked')) {
        oldBordersLayer = L.layerGroup();
        map.addLayer(oldBordersLayer);
        updateBorders();
    }
    else if (oldBordersLayer != null) {
        map.removeLayer(oldBordersLayer);
        oldBordersLayer = null;
    }
}

function importInJOSM(method, data) {
    var url = getServer(method) + '?' + $.param(data);
    var params = [
        ['new_layer', 'true'],
        ['format', '.osm'],
        ['layer_name', 'borders_' + Date.now()],
        ['url', url]
    ];
    var params_str = params.map(x => (x[0] + '=' + x[1])).join('&');
    $.ajax({
        url: JOSM_IMPORT_LISTEN_ADDRESS + '?' + encodeURI(params_str),
        // Don't use ajax 'data' param since the order of
        // params in url matters: url=<url> must be the last
        // otherwise all the rest params would be a part of that url.
        complete: function(t) {
            if (t.status != 200)
                window.alert(
                'Please enable remote_control in JOSM');
        }
    });
}

function bJOSM() {
    var b = map.getBounds();
    importInJOSM('josm', {
        'xmin': b.getWest(),
        'xmax': b.getEast(),
        'ymin': b.getSouth(),
        'ymax': b.getNorth()
    });
}

function bJosmOld() {
    var b = map.getBounds();
    importInJOSM('josm', {
        'table': config.OLD_BORDERS_NAME,
        'xmin': b.getWest(),
        'xmax': b.getEast(),
        'ymin': b.getSouth(),
        'ymax': b.getNorth()
    });
}

function bJosmZoom() {
    var b = map.getBounds();
    $.ajax({
        url: 'http://127.0.0.1:8111/zoom',
        data: {
            'left': b.getWest(),
            'right': b.getEast(),
            'bottom': b.getSouth(),
            'top': b.getNorth()
        }
    });
}

function bImport() {
    if ($('#filefm input[type=file]').val()) {
        document.querySelector('#filefm').submit();
        var frame_doc = $('#h_iframe')[0].contentWindow.document;
        var frame_body = $('body', frame_doc);
        frame_body.html('<pre>Идёт загрузка...</pre>');
        $('#import_div').show();
    }
}

function finishRename() {
    $('#rename_link').html('Название &#9660');
    $('#rename').hide();
}

function bToggleRename() {
    if (!selectedId || !(selectedId in borders) || config.READONLY)
        return;
    var rename_el = $('#rename');
    if (rename_el.is(':hidden')) {
        $('#b_rename').val(borders[selectedId].name);
        $('#rename_link').html('Название &#9650');
        rename_el.show();
    }
    else {
        finishRename();
    }
}

function bRename() {
    if (!selectedId || !(selectedId in borders))
        return;
    $.ajax(getServer('rename'), {
        data: {
            'id': selectedId,
            'new_name': $('#b_rename').val()
        },
        success: makeAnswerHandler(function() {
            finishRename();
            updateBorders();
        })
    });
}

function bDisable() {
    if (!selectedId || !(selectedId in borders))
        return;
    $.ajax(getServer(borders[selectedId].disabled ? 'enable' : 'disable'), {
        data: {
            'id': selectedId
        },
        success: updateBorders
    });
}

function bSimpleSplit() {
    if (!selectedId || !(selectedId in borders))
        return;
    $.ajax(getServer('simple_split'), {
        data: {
            'id': selectedId
        },
        success: makeAnswerHandler(updateBorders)
    });
}

function bDelete() {
    if (!selectedId || !(selectedId in borders))
        return;
    var name = borders[selectedId].name;
    if (!window.confirm('Точно удалить регион ' +
            name + ' (' + selectedId + ')' + '?'))
        return;
    $.ajax(getServer('delete'), {
        data: {
            'id': selectedId
        },
        success: updateBorders
    });
}

var selectedIdForParentAssigning = null;
var potentialParentLayer = null;
var potentialParentLayers = {};
var potentialParents = null;

function finishChooseParent() {
    if (potentialParentLayer) {
        map.removeLayer(potentialParentLayer);
        potentialParentLayer = null;
    }
    potentialParentLayers = {};
    potentialParents = {};
    selectedIdForParentAssigning = null;
    $('#potential_parents').empty().hide();
    $('#parent_link').html('Родитель &#9660:');
}

function bTogglePotentialParents() {
    var potentialParentsDiv = $('#potential_parents');
    if (potentialParentsDiv.is(':visible')) {
        finishChooseParent();
    }
    else {
        if (!selectedId || !(selectedId in borders))
            return;
        selectedIdForParentAssigning = selectedId;
        $('#parent_link').html('Родитель &#9650:');
        potentialParentsDiv.html('Ожидайте...').show();
        $.ajax(getServer('potential_parents'), {
            data: {
                'id': selectedIdForParentAssigning
            },
            success: processPotentialParents
        });
    }
}

function makeShowParent(parent_id) {
    return function(event) {
        event.preventDefault();
        if (!(parent_id in potentialParentLayers)) {
            potentialParentLayers[parent_id] = L.geoJson(
                potentialParents[parent_id], {
                    style: STYLE_PARENT
                });
        }
        if (potentialParentLayer) {
            map.removeLayer(potentialParentLayer);
        }
        potentialParentLayer = potentialParentLayers[parent_id];
        map.addLayer(potentialParentLayer);
        potentialParentLayer.bringToFront();
    };
}

function makeSetParent(parent_id) {
    return function(event) {
        event.preventDefault();
        $.ajax(getServer('set_parent'), {
            data: {
                'id': selectedIdForParentAssigning,
                'parent_id': parent_id
            },
            success: makeAnswerHandler(function() {
                updateBorders();
                finishChooseParent();
            })
        });
    };
}

function processPotentialParents(answer) {
    if (!selectedIdForParentAssigning || !(selectedIdForParentAssigning in
            borders))
        return;
    var parents = answer.parents;
    potentialParents = {};
    var potentialParentsDiv = $('#potential_parents');
    if (parents.length == 0) {
        potentialParentsDiv.html('Ничего не найдено.');
        return;
    }
    potentialParentsDiv.html('');
    var selectedParentId = borders[selectedIdForParentAssigning].parent_id;
    for (var i = 0; i < parents.length; ++i) {
        var parent = parents[i];
        var parent_id = parent.properties.id;
        potentialParents[parent_id] = parent;
        var div = $('<div/>').appendTo(potentialParentsDiv);
        var al = parent.properties.admin_level;
        var al_str = al ? ' (' + al + ')' : '';
        var name = (parent.properties.name || '' + parent_id) + al_str;
        $('<span>' + name + '</span>').appendTo(div);
        $('<span> </span>').appendTo(div);
        $('<a href="#">показать</a>')
            .click(makeShowParent(parent_id))
            .appendTo(div);
        var isCurrentParent = (parent_id === selectedParentId);
        $('<span> </span>').appendTo(div);
        $('<a href="#">' + (isCurrentParent ? 'отвязать' : 'назначить') +
                '</a>')
            .click(makeSetParent(isCurrentParent ? null : parent_id))
            .appendTo(div);
    }
}

function sendComment(text) {
    if (!selectedId || !(selectedId in borders))
        return;
    $.ajax(getServer('comment'), {
        data: {
            'id': selectedId,
            'comment': text
        },
        type: 'POST',
        success: updateBorders
    });
}

function bComment() {
    sendComment($('#b_comment').val());
}

function bClearComment() {
    $('#b_comment').val('');
    sendComment('');
}

var splitLayer = null,
    splitSelected = null;

function bSplit() {
    if (!selectedId || !(selectedId in borders))
        return;
    splitSelected = selectedId;
    var name = borders[selectedId].name;
    $('#s_sel').text(name + ' (' + selectedId + ')');
    $('#actions').css('display', 'none');
    $('#split').css('display', 'block');
    map.on('editable:drawing:end', bSplitDrawn);
    bSplitStart();
}

function bSplitStart() {
    $('#s_do').css('display', 'none');
    splitLayer = null;
    map.editTools.startPolyline();
}

function bSplitDrawn(e) {
    splitLayer = e.layer;
    $('#s_do').css('display', 'block');
}

function bSplitAgain() {
    map.editTools.stopDrawing();
    if (splitLayer != null)
        map.removeLayer(splitLayer);
    bSplitStart();
}

function bSplitDo() {
    var wkt = '',
        lls = splitLayer.getLatLngs();
    for (var i = 0; i < lls.length; i++) {
        if (i > 0)
            wkt += ',';
        wkt += L.Util.formatNum(lls[i].lng, 6) + ' ' + L.Util.formatNum(lls[i]
            .lat, 6);
    }
    $.ajax(getServer('split'), {
        data: {
            'id': splitSelected,
            'line': 'LINESTRING(' + wkt + ')',
            'save_region': $('#save_split_region').prop('checked')
        },
        datatype: 'json',
        success: makeAnswerHandler(updateBorders)
    });
    bSplitCancel();
}

function bSplitJosm() {
    var wkt = '',
        lls = splitLayer.getLatLngs();
    for (i = 0; i < lls.length; i++) {
        if (i > 0)
            wkt += ',';
        wkt += L.Util.formatNum(lls[i].lng, 6) + ' ' + L.Util.formatNum(lls[i]
            .lat, 6);
    }
    importInJOSM('josmbord', {
        'id': splitSelected,
        'line': 'LINESTRING(' + wkt + ')'
    });
}

function bSplitCancel() {
    map.editTools.stopDrawing();
    if (splitLayer != null)
        map.removeLayer(splitLayer);
    $('#split').hide();
    $('#actions').show();
}

var joinSelected = null,
    joinAnother = null;

function bJoin() {
    if (!selectedId || !(selectedId in borders))
        return;
    joinSelected = selectedId;
    joinAnother = null;
    var name = borders[selectedId].name;
    $('#j_sel').text(name + '(' + selectedId + ')');
    $('#actions').css('display', 'none');
    $('#j_do').css('display', 'none');
    $('#join').css('display', 'block');
}

// called from selectLayer() when joinSelected is not null
function bJoinSelect(layer) {
    if ('id' in layer && layer.id in borders && layer.id != joinSelected) {
        joinAnother = layer.id;
        var name2 = borders[joinAnother].name;
        $('#j_name2').text(name2 + '(' + joinAnother + ')');
        $('#j_do').css('display', 'block');
    }
}

function bJoinDo() {
    if (joinSelected != null && joinAnother != null) {
        $.ajax(getServer('join'), {
            data: {
                'id1': joinSelected,
                'id2': joinAnother
            },
            success: makeAnswerHandler(updateBorders)
        });
    }
    bJoinCancel();
}

function bJoinCancel() {
    joinSelected = null;
    $('#join').hide();
    $('#actions').show();
}

var parentLayer = null;

function getParentBorderRepresentation(props, long) {
    var parent_repr = '';

    if (props['parent_id']) {
        parent_repr = props['parent_name'] || '' + props['parent_id'];

        if (long && props['parent_name'])
            parent_repr += ' [' + props['parent_id'] + ']';

        if (props['parent_admin_level'])
            parent_repr += ' (' + props['parent_admin_level'] + ')';
    }
    return parent_repr;
}

function bJoinToParent() {
    if (!selectedId || !(selectedId in borders))
        return;
    var props = borders[selectedId];
    if (!props['parent_id']) {
        alert('Это регион верхнего уровня');
        return;
    }
    joinSelected = selectedId;
    $('#j_to_parent_sel').text(props['name'] + ' (' + selectedId + ')');
    $('#j_sel_parent').text(getParentBorderRepresentation(props, true));
    $('#actions').hide();
    $('#join_to_parent').show();
}

function bJoinToParentPreview() {
    if (parentLayer != null) {
        map.removeLayer(parentLayer);
        parentLayer = null;
    }
    var props = borders[selectedId];
    var simplified = map.getZoom() < 7 ? 2 : (map.getZoom() < 11 ? 1 : 0);
    $.ajax(getServer('border'), {
        data: {
            'id': props['parent_id'],
            'simplify': simplified
        },
        success: makeAnswerHandler(processJoinToParentPreview)
    });
}

function processJoinToParentPreview(answer) {
    parentLayer = L.geoJson(answer.geojson, {
        style: STYLE_PARENT
    });
    map.addLayer(parentLayer);
    parentLayer.bringToFront();
}

function bJoinToParentDo() {
    if (joinSelected != null) {
        $.ajax(getServer('join_to_parent'), {
            data: {
                'id': joinSelected
            },
            success: makeAnswerHandler(updateBorders)
        });
    }
    bJoinToParentCancel();
}

function bJoinToParentCancel() {
    joinSelected = null;
    if (parentLayer != null) {
        map.removeLayer(parentLayer);
        parentLayer = null;
    }
    $('#actions').show();
    $('#join_to_parent').hide();
}

var pMarker = L.marker([0, 0], {
    draggable: true
});

function bPoint() {
    $('#p_name').val('*');
    selectLayer(null);
    $('#actions').css('display', 'none');
    $('#point').css('display', 'block');
    pMarker.setLatLng(map.getCenter());
    map.addLayer(pMarker);
}

function bPointList() {
    var ll = pMarker.getLatLng();
    $.ajax(getServer('point'), {
        data: {
            'lat': ll.lat,
            'lon': ll.lng
        },
        dataType: 'json',
        success: updatePointList
    });
}

function updatePointList(data) {
    var list = $('#p_list');
    list.text('');
    if (!data || !('borders' in data))
        return;
    for (var i = 0; i < data.borders.length; i++) {
        var b = data.borders[i];
        var a = document.createElement('a');
        a.href = '#';
        a.onclick = (function(id, name) {
            return function() {
                pPointSelect(id, name);
                return false;
            };
        })(b['id'], b['name']);
        list.append(a);
        $(a).text(b['admin_level'] + ': ' + b['name'] + ' (' +
                      Math.round(b['area']) + ' км²)');
        if (b['admin_level'] == 2) {
            var auto_divide_link = document.createElement('a');
            auto_divide_link.href = '#';
            auto_divide_link.onclick = (function(id) {
                return function() {
                    pAutoDivideCountry(id);
                    return false;
                };
            })(b['id']);
            $(auto_divide_link).text("!!autodivide!!");
            list.append("<br>&nbsp;&nbsp;&nbsp;", auto_divide_link);
        }
        list.append("<br>");
    }
}

function pPointSelect(id, osm_name) {
    var name = $('#p_name').val();
    name = name.replace('*', osm_name);
    $.ajax(getServer('from_osm'), {
        data: {
            'name': name,
            'id': id
        },
        success: makeAnswerHandler(updateBorders)
    });
    bPointCancel();
}

function pAutoDivideCountry(id) {
    $.ajax(getServer('auto_divide_country'), {
        data: {'id': id},
        success: makeAnswerHandler(updateBorders)
    });
    bPointCancel();
}

function bPointCancel() {
    $('#point').hide();
    $('#actions').show();
    $('#p_list').text('');
    map.removeLayer(pMarker);
}

var subregionsLayer = null,
    clustersLayer = null,
    divSelectedId = null;

function bDivide() {
    if (!selectedId || !(selectedId in borders))
        return;
    divSelectedId = selectedId;
    $('#actions').hide();
    $('#d_count').hide();
    $('#b_divide_do').hide();
    $('#divide').show();

    $('#region_to_divide').text(borders[divSelectedId].name + ' (' +
        divSelectedId + ')');
    var next_admin_level = borders[divSelectedId].admin_level ?
        borders[divSelectedId].admin_level + 1 : null;
    $('#next_level').val(next_admin_level);

    if (next_admin_level === null) {
        // Divided region is not an administrative unit.
        // Do not allow 'auto_divide' and 'apply_to_similar' options.
        $('#auto_divide').prop({
            'checked': false,
            'disabled': true
        });
        $('#apply_to_similar').prop({
            'checked': false,
            'disabled': true
        });
    }
    else {
        $('#auto_divide').prop('disabled', false);
        $('#apply_to_similar').prop('disabled', false);
    }
}

function clearDivideLayers() {
    if (subregionsLayer != null) {
        map.removeLayer(subregionsLayer);
        subregionsLayer = null;
    }
    if (clustersLayer != null) {
        map.removeLayer(clustersLayer);
        clustersLayer = null;
    }
}

function bDividePreview() {
    var auto_divide = $('#auto_divide').prop('checked');
    if (auto_divide && !$('#mwm_size_thr').val()) {
        alert('Fill mmw size threshold');
        return;
    }
    clearDivideLayers();
    $('#d_count').hide();
    $('#b_divide_do').hide();
    var apply_to_similar = $('#apply_to_similar').prop('checked');
    var params = {
        'id': divSelectedId,
        'next_level': $('#next_level').val(),
        'auto_divide': auto_divide,
        'apply_to_similar': apply_to_similar
    };
    if (auto_divide) {
        params['mwm_size_thr'] = parseInt($('#mwm_size_thr').val()) * 1024;
    }
    $.ajax(getServer('divide_preview'), {
        data: params,
        success: makeAnswerHandler(bDivideDrawPreview)
    });
}

function bDivideDrawPreview(response) {
    var subregions = response.subregions;
    var clusters = response.clusters;
    if (!('features' in subregions) || !subregions.features.length) {
        $('#d_count').text('Нет областей').show();
        return;
    }
    subregionsLayer = L.geoJson(subregions, {
        style: function(f) {
            return {
                color: 'blue',
                weight: 1,
                fill: false
            };
        }
    });
    map.addLayer(subregionsLayer);
    subregionsLayer.bringToFront();
    if (clusters) {
        clustersLayer = L.geoJson(clusters, {
            style: function(f) {
                return {
                    color: 'black',
                    weight: 2,
                    fill: false
                };
            }
        });
        map.addLayer(clustersLayer);
        clustersLayer.bringToFront();
    }
    var subregions_count_text = '' + subregions.features.length +
    ' подрегионов';
    var show_divide_button = (subregions.features.length > 1);
    if (clusters) {
        subregions_count_text += ', ' + clusters.features.length + ' кластеров';
        show_divide_button = (clusters.features.length > 0);
    }
    $('#d_count').text(subregions_count_text).show();
    if (show_divide_button)
        $('#b_divide_do').show();
    else
        $('#b_divide_do').hide();
}

function bDivideDo() {
    var auto_divide = $('#auto_divide').prop('checked');
    var apply_to_similar = $('#apply_to_similar').prop('checked');
    var params = {
        'id': divSelectedId,
        'next_level': $('#next_level').val(),
        'auto_divide': auto_divide,
        'apply_to_similar': apply_to_similar
    };
    if (auto_divide) {
        params['mwm_size_thr'] = parseInt($('#mwm_size_thr').val()) * 1024;
    }
    $.ajax(getServer('divide'), {
        data: params,
        success: updateBorders
    });
    bDivideCancel();
}

function bDivideCancel() {
    clearDivideLayers();
    divSelectedId = null;
    $('#divide').hide();
    $('#actions').show();
}

function bLargest() {
    if (!selectedId || !(selectedId in borders))
        return;
    $.ajax(getServer('chop1'), {
        data: {
            'id': selectedId
        },
        success: makeAnswerHandler(updateBorders)
    });
}

function bHull() {
    if (!selectedId || !(selectedId in borders))
        return;
    $.ajax(getServer('hull'), {
        data: {
            'id': selectedId
        },
        success: makeAnswerHandler(updateBorders)
    });
}

function bBackup() {
    $('#actions').css('display', 'none');
    $('#backup_saving').css('display', 'none');
    $('#backup_restoring').css('display', 'none');
    $('#backup_save').attr('disabled', false);
    $('#backup_list').text('');
    $('#backup').css('display', 'block');
    $.ajax(getServer('backlist'), {
        success: updateBackupList
    });
}

function bBackupCancel() {
    $('#actions').css('display', 'block');
    $('#backup').css('display', 'none');
}

function updateBackupList(data) {
    var list = $('#backup_list');
    list.text('');
    if (!data || !('backups' in data))
        return;
    for (var i = 0; i < data.backups.length; i++) {
        var b = data.backups[i];
        var a = document.createElement('a');
        a.href = '#';
        a.onclick = (function(id, name) {
            return function() {
                bBackupRestore(id);
                return false
            }
        })(b['timestamp']);
        $(a).text(b['text'] + ' (' + b['count'] + ')');
        if (i > 0) {
            var d = document.createElement('a');
            d.className = 'back_del';
            d.href = '#';
            d.onclick = (function(id, name) {
                return function() {
                    bBackupDelete(id);
                    return false
                }
            })(b['timestamp']);
            $(d).text('[x]');
            list.append(a, document.createTextNode(' '), d, $('<br>'));
        }
        else {
            list.append(a, $('<br>'));
        }
    }
}

function bBackupSave() {
    $.ajax(getServer('backup'), {
        success: bBackupCancel
    });
    $('#backup_save').attr('disabled', true);
    $('#backup_saving').css('display', 'block');
}

function bBackupRestore(timestamp) {
    $.ajax(getServer('restore'), {
        data: {
            'timestamp': timestamp
        },
        success: function() {
            bBackupCancel();
            updateBorders();
        }
    });
    $('#backup_list').text('');
    $('#backup_restoring').css('display', 'block');
}

function bBackupDelete(timestamp) {
    $.ajax(getServer('backdelete'), {
        data: {
            'timestamp': timestamp
        }
    });
    bBackupCancel();
}

function getPolyDownloadLink(use_bbox) {
    var downloadLink = getServer('poly');

    if (use_bbox) {
        var b = map.getBounds();
        var data = {
            'xmin': b.getWest(),
            'xmax': b.getEast(),
            'ymin': b.getSouth(),
            'ymax': b.getNorth()
        };
        downloadLink += '?' + $.param(data);
    }

    return downloadLink;
}

function startOver() {
    if (confirm('Вы уверены, что хотите начать разбиение сначала?')) {
        finishChooseParent();
        bSplitCancel();
        bJoinCancel();
        bJoinToParentCancel();
        bPointCancel();
        bDivideCancel();
        bBackupCancel();
        selectLayer(null);
        $('#wait_start_over').show();
        $.ajax(getServer('start_over'), {
            success: makeAnswerHandler(function() {
                for (var id in borders) {
                    bordersLayer.removeLayer(borders[id].layer);
                    delete borders[id];
                }
                updateBorders();
            }),
            complete: function() {
                $('#wait_start_over').hide();
            }

        });
    }
}
