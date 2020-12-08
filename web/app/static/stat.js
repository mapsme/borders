var MB_LIMIT = 50,
    MB_LIMIT2 = 70;
var KM_LIMIT = 50,
    POINT_LIMIT = 50000;

function statInit() {
    $('.mb_limit').text(MB_LIMIT);
    $('.mb_limit2').text(MB_LIMIT2);
    $('.km_limit').text(KM_LIMIT);
    $('.point_limit').text(Math.round(POINT_LIMIT / 1000));
    statQuery('total', statTotal);
}

function statOpen(id) {
    var div = document.getElementById(id);
    if (div.style.display != 'block')
        div.style.display = 'block';
    else
        div.style.display = 'none';
}

function statQuery(id, callback) {
    $.ajax(getServer('stat'), {
        data: {
            'group': id
        },
        success: function(data) {
            callback(data);
            document.getElementById(id).style.display = 'block';
        },
        error: function() {
            alert('Failed!');
        }
    });

}

function formatNum(value, digits) {
    if (digits != undefined) {
        var pow = Math.pow(10, digits);
        return Math.round(value * pow) / pow;
    }
    else
        return value;
}

function statFill(id, value, digits) {
    document.getElementById(id).innerHTML = ('' + formatNum(value, digits))
        .replace('&', '&amp;').replace('<', '&lt;');
}

function getIndexLink(region) {
    var big = region.area > 1000;
    return 'index.html#' + (big ? 8 : 12) + '/' + region.lat + '/' + region.lon;
}

function statFillList(id, regions, comment, count) {
    var div = document.getElementById(id),
        i, a, html, p;
    if (!div) {
        console.log('Div ' + id + ' not found');
        return;
    }
    if (count)
        statFill(count, regions.length);
    for (i = 0; i < regions.length; i++) {
        a = document.createElement('a');
        a.href = getIndexLink(regions[i]);
        a.target = '_blank';
        html = regions[i].name;
        if (comment) {
            if (typeof comment == 'string')
                p = regions[i][comment];
            else
                p = comment(regions[i]);
            if (p)
                html += ' (' + p + ')';
        }
        a.innerHTML = html.replace('&', '&amp;').replace('<', '&lt;');
        div.appendChild(a);
        div.appendChild(document.createElement('br'));
    }
}

function statTotal(data) {
    statFill('total_total', data.total);
    statQuery('sizes', statSizes);
}

function statSizes(data) {
    var list_1mb = [],
        list_50mb = [],
        list_100mb = [];
    var list_spaces = [],
        list_bad = [];
    var list_100km = [],
        list_100kp = [],
        list_zero = [];
    var list_100p = [];
    var list_disabled = [],
        list_commented = [];

    for (var i = 0; i < data.regions.length; i++) {
        region = data.regions[i];
        if (region.area > 0 && region.area < KM_LIMIT)
            list_100km.push(region);
        if (region.area <= 0)
            list_zero.push(region);
        if (region.nodes > POINT_LIMIT)
            list_100kp.push(region);
        if (region.nodes < 50)
            list_100p.push(region);
        var size_mb = region.size * window.BYTES_FOR_NODE / 1024 / 1024;
        region.size_mb = size_mb;
        if (size_mb < 1)
            list_1mb.push(region);
        if (size_mb > MB_LIMIT)
            list_50mb.push(region);
        if (size_mb > MB_LIMIT2)
            list_100mb.push(region);
        if (!/^[\x20-\x7F]*$/.test(region.name))
            list_bad.push(region);
        if (region.name.indexOf(' ') >= 0)
            list_spaces.push(region);
        if (region.disabled)
            list_disabled.push(region);
        if (region.commented)
            list_commented.push(region);
    }

    statFill('names_spaces', list_spaces.length);
    statFillList('names_bad_list', list_bad, null, 'names_bad');
    statFillList('total_disabled_list', list_disabled, null, 'total_disabled');
    statFillList('total_commented_list', list_commented, null,
        'total_commented');

    list_1mb.sort(function(a, b) {
        return a.size_mb - b.size_mb;
    });
    list_50mb.sort(function(a, b) {
        return a.size_mb - b.size_mb;
    });
    list_100mb.sort(function(a, b) {
        return b.size_mb - a.size_mb;
    });
    statFillList('sizes_1mb_list', list_1mb, function(r) {
        return formatNum(r.size_mb, 2) + ' МБ';
    }, 'sizes_1mb');
    statFillList('sizes_50mb_list', list_50mb, function(r) {
        return formatNum(r.size_mb, 0) + ' МБ';
    }, 'sizes_50mb');
    statFillList('sizes_100mb_list', list_100mb, function(r) {
        return formatNum(r.size_mb, 0) + ' МБ';
    }, 'sizes_100mb');

    list_100km.sort(function(a, b) {
        return a.area - b.area;
    });
    list_100kp.sort(function(a, b) {
        return b.nodes - a.nodes;
    });
    list_100p.sort(function(a, b) {
        return a.nodes - b.nodes;
    });
    statFillList('areas_100km_list', list_100km, function(r) {
        return formatNum(r.area, 2) + ' км²';
    }, 'areas_100km');
    statFillList('areas_50k_points_list', list_100kp, 'nodes',
        'areas_50k_points');
    statFillList('areas_100_points_list', list_100p, 'nodes',
        'areas_100_points');
    statFillList('areas_0_list', list_zero, null, 'areas_0');

    statQuery('topo', statTopo);
}

function statTopo(data) {
    var list_holed = [],
        list_multi = [],
        list_100km = [];
    for (var i = 0; i < data.regions.length; i++) {
        region = data.regions[i];
        if (region.outer > 1)
            list_multi.push(region);
        if (region.inner > 0)
            list_holed.push(region);
        if (region.outer > 1 && region.min_area > 0 && region.min_area <
            KM_LIMIT)
            list_100km.push(region);
    }

    list_multi.sort(function(a, b) {
        return b.outer - a.outer;
    });
    list_holed.sort(function(a, b) {
        return b.inner - a.inner;
    });
    list_100km.sort(function(a, b) {
        return a.min_area - b.min_area;
    });
    statFillList('topo_holes_list', list_holed, 'inner', 'topo_holes');
    statFillList('topo_multi_list', list_multi, 'outer', 'topo_multi');
    statFillList('topo_100km_list', list_100km, function(r) {
        return formatNum(r.min_area, 2) + ' км²';
    }, 'topo_100km');
}
