import argparse
from datetime import datetime,timezone
import logging
import xml.etree.ElementTree as ET

from xml.dom import minidom
from types import SimpleNamespace

from ._tool_descriptor import Tool
from .utils.reader import Reader, units


namespace_urls = {
    '': "http://www.topografix.com/GPX/1/1",
    'xsi': "http://www.w3.org/2001/XMLSchema-instance",
    'tpx': "http://www.garmin.com/xmlschemas/TrackPointExtension/v2",
    'adx': "http://www.n3r1.com/xmlschemas/ActivityDataExtensions/v1",
}

namespace_schemas = {
    '': "http://www.topografix.com/GPX/1/1/gpx.xsd",
    'tpx': "http://www.garmin.com/xmlschemas/TrackPointExtensionv2.xsd",
    'adx': "http://www.n3r1.com/xmlschemas/ActivityDataExtensionsv1.xsd",
}

tag = SimpleNamespace(
    gpx="{" + namespace_urls[''] + "}",
    tpx="{" + namespace_urls['tpx'] + "}",
    adx="{" + namespace_urls['adx'] + "}",
)


def gpx_ts(dt: datetime|None) -> str:
    if dt is None:
        return ""
    if dt.tzinfo is None:
        dt_utc = dt.replace(tzinfo=timezone.utc)
    else:
        dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


def main(fit_file: str, output: str|None = None) -> bool:
    reader = Reader(fit_file)
    if not reader.ok:
        logging.error("Failed to read fit file.")
        return False

    if output is None:
        output = fit_file.rsplit('.', 1)[0] + '.gpx'

    logging.info(f"Converting file {fit_file} to {output}")

    for key, url in namespace_urls.items():
        ET.register_namespace(key, url)

    gpx = ET.Element(f"{tag.gpx}gpx", {
        'version': "1.1",
        'creator': "fitt",
        f"{tag.gpx}schemaLocation": " ".join([f"{namespace_urls[key]} {namespace_schemas[key]}" for key in namespace_schemas.keys()])
    })

    # METADATA
    metadata = ET.SubElement(gpx, f"{tag.gpx}metadata")
    ET.SubElement(metadata, f"{tag.gpx}link", {'href': "https://github.com/neri14/fitt"})
    if 'start_time' in reader.metadata:
        ET.SubElement(metadata, f"{tag.gpx}time").text = gpx_ts(reader.metadata['start_time'])
    if 'minlat' in reader.metadata and 'minlon' in reader.metadata and 'maxlat' in reader.metadata and 'maxlon' in reader.metadata:
        ET.SubElement(metadata, f"{tag.gpx}bounds", {
            'minlat': str(reader.metadata['minlat']),
            'minlon':  str(reader.metadata['minlon']),
            'maxlat': str(reader.metadata['maxlat']),
            'maxlon': str(reader.metadata['maxlon']),
        })
    #METADATA END

    #TODO support for wpt
    #TODO support for rte

    #TRACK
    trk = ET.SubElement(gpx, f"{tag.gpx}trk")
    ET.SubElement(trk, f"{tag.gpx}name").text = reader.metadata['activity_name'] if 'activity_name' in reader.metadata else "Unnamed Activity"

    if 'device' in reader.metadata:
        ET.SubElement(trk, f"{tag.gpx}src").text = reader.metadata['device']

    track_type = reader.metadata['sport'] if 'sport' in reader.metadata else "other"
    if 'sub_sport' in reader.metadata and reader.metadata['sub_sport'] != 'generic':
        track_type = f"{reader.metadata['sub_sport']}_{track_type}"
    ET.SubElement(trk, f"{tag.gpx}type").text = track_type


    trk_ext = ET.SubElement(trk, f"{tag.gpx}extensions")
    trk_adx = ET.SubElement(trk_ext, f"{tag.adx}ActivityTrackExtension")
    
    if 'total_elapsed_time' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}elapsedtime").text = str(reader.metadata['total_elapsed_time'])
    if 'total_timer_time' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}timertime").text = str(reader.metadata['total_timer_time'])
    if 'total_distance' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}distance").text = str(reader.metadata['total_distance'])
    if 'total_ascent' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}ascent").text = str(reader.metadata['total_ascent'])
    if 'total_descent' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}descent").text = str(reader.metadata['total_descent'])
    if 'total_cycles' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}cycles").text = str(reader.metadata['total_cycles'])
    if 'total_strokes' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}strokes").text = str(reader.metadata['total_strokes'])
    if 'total_work' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}work").text = str(reader.metadata['total_work'])
    if 'total_calories' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}kcal").text = str(reader.metadata['total_calories'])

    if 'total_grit' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}grit").text = str(reader.metadata['total_grit'])
    if 'avg_flow' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}flow").text = str(reader.metadata['avg_flow'])
    
    if 'avg_speed' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}avgspeed").text = str(reader.metadata['avg_speed'])
    if 'max_speed' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}maxspeed").text = str(reader.metadata['max_speed'])
    
    if 'avg_power' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}avgpower").text = str(reader.metadata['avg_power'])
    if 'max_power' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}maxpower").text = str(reader.metadata['max_power'])
    if 'normalized_power' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}normpower").text = str(reader.metadata['normalized_power'])

    if 'avg_vam' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}avgvam").text = str(reader.metadata['avg_vam'])

    if 'avg_respiration_rate' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}avgrr").text = str(reader.metadata['avg_respiration_rate'])
    if 'max_respiration_rate' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}maxrr").text = str(reader.metadata['max_respiration_rate'])
    if 'min_respiration_rate' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}minrr").text = str(reader.metadata['min_respiration_rate'])
    
    if 'jump_count' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}jumps").text = str(reader.metadata['jump_count'])

    if 'avg_heart_rate' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}avghr").text = str(reader.metadata['avg_heart_rate'])
    if 'max_heart_rate' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}maxhr").text = str(reader.metadata['max_heart_rate'])

    if 'avg_cadence' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}avgcad").text = str(reader.metadata['avg_cadence'])
    if 'max_cadence' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}maxcad").text = str(reader.metadata['max_cadence'])

    if 'avg_temperature' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}avgatemp").text = str(reader.metadata['avg_temperature'])
    if 'max_temperature' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}maxatemp").text = str(reader.metadata['max_temperature'])
    if 'min_temperature' in reader.metadata:
        ET.SubElement(trk_adx, f"{tag.adx}minatemp").text = str(reader.metadata['min_temperature'])

    # TRACK SEGMENT
    trkseg = ET.SubElement(trk, f"{tag.gpx}trkseg")
    
    for timestamp,record in reader.data:
        # TRACK POINT
        if 'position_lat' not in record or 'position_long' not in record:
            logging.warning("Skipping record without position when generating gpx file")
            continue

        trkpt = ET.SubElement(trkseg, f"{tag.gpx}trkpt",
                              lat=str(record['position_lat']),
                              lon=str(record['position_long']))

        if 'altitude' in record:
            ET.SubElement(trkpt, f"{tag.gpx}ele").text = str(record['altitude'])

        ET.SubElement(trkpt, f"{tag.gpx}time").text = gpx_ts(timestamp)

        # TRACK POINT EXTENSIONS
        trkpt_ext = ET.SubElement(trkpt, f"{tag.gpx}extensions")

        # TrackPointExtension
        trkpt_tpx = ET.SubElement(trkpt_ext, f"{tag.tpx}TrackPointExtension")

        if 'temperature' in record:
            ET.SubElement(trkpt_tpx, f"{tag.tpx}atemp").text = str(record['temperature'])
        if 'heart_rate' in record:
            ET.SubElement(trkpt_tpx, f"{tag.tpx}hr").text = str(record['heart_rate'])
        if 'cadence' in record:
            ET.SubElement(trkpt_tpx, f"{tag.tpx}cad").text = str(record['cadence'])
        if 'speed' in record:
            ET.SubElement(trkpt_tpx, f"{tag.tpx}speed").text = str(record['speed'])

        # ActivityPointExtension
        trkpt_adx = ET.SubElement(trkpt_ext, f"{tag.adx}ActivityTrackPointExtension")

        if 'time' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}time").text = str(record['time'])
        if 'smooth_altitude' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}smoothele").text = str(record['smooth_altitude'])
        if 'distance' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}dist").text = str(record['distance'])
        if 'calories' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}kcal").text = str(record['calories'])

        if 'respiration_rate' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}rr").text = str(record['respiration_rate'])
        if 'core_temperature' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}ctemp").text = str(record['core_temperature'])

        if 'power' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}power").text = str(record['power'])
        if 'power3s' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}power3s").text = str(record['power3s'])
        if 'power10s' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}power10s").text = str(record['power10s'])
        if 'power30s' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}power30s").text = str(record['power30s'])
        if 'accumulated_power' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}accpower").text = str(record['accumulated_power'])

        if 'grade' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}grade").text = str(record['grade'])
        if 'vertical_speed' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}vspeed").text = str(record['vertical_speed'])

        if 'left_right_balance' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}lrbalance").text = str(record['left_right_balance'])
        if 'left_torque_effectiveness' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}ltrqeff").text = str(record['left_torque_effectiveness'])
        if 'right_torque_effectiveness' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}rtrqeff").text = str(record['right_torque_effectiveness'])
        if 'left_pedal_smoothness' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}lpdlsmooth").text = str(record['left_pedal_smoothness'])
        if 'right_pedal_smoothness' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}rpdlsmooth").text = str(record['right_pedal_smoothness'])
        if 'combined_pedal_smoothness' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}cpdlsmooth").text = str(record['combined_pedal_smoothness'])

        if 'grit' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}grit").text = str(record['grit'])
        if 'flow' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}flow").text = str(record['flow'])

        if 'active_climb' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}climb").text = str(record['active_climb'])

        if 'front_gear_num' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}fgearnum").text = str(record['front_gear_num'])
        if 'front_gear' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}fgear").text = str(record['front_gear'])
        if 'rear_gear_num' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}rgearnum").text = str(record['rear_gear_num'])
        if 'rear_gear' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}rgear").text = str(record['rear_gear'])

        if 'jump_distance' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}jumpdist").text = str(record['jump_distance'])
        if 'jump_height' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}jumpheight").text = str(record['jump_height'])
        if 'jump_hang_time' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}jumptime").text = str(record['jump_hang_time'])
        if 'jump_score' in record:
            ET.SubElement(trkpt_adx, f"{tag.adx}jumpscore").text = str(record['jump_score'])


    rough = ET.tostring(gpx, 'utf-8')
    pretty = minidom.parseString(rough).toprettyxml(indent="  ")
    with open(output, "w", encoding="utf-8") as f:
        f.write(pretty)

    return True


def add_argparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "convert",
        help="Convert the FIT file to GPX." #TODO add more formats in future
    )
    parser.add_argument(
        "fit_file",
        help="Path to the FIT file."
    )
    parser.add_argument(
        "-o", "--output",
        help="Path to the output file. If not provided, uses the same name as the input file changed extension.",
        default=None
    )

tool = Tool(
    name="convert",
    description="Convert the FIT file to GPX.",
    add_argparser=add_argparser,
    main=main
)
