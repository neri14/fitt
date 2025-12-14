import logging
import math
import numpy as np
import statistics
import time
from datetime import datetime, timedelta
from garmin_fit_sdk import Decoder, Stream, Profile
from typing import Generator

from .geo import geo_distance


SEMICIRCLES_FACTOR = 180.0 / 2**31
SMOOTH_ALTITUDE_TIME_WINDOW = 5 # seconds
MAX_GRADE_WINDOW = 50 # meters
MIN_GRADE_WINDOW = 20 # meters


units = {
    'time':                             's',
    'timestamp':                        None,
    'position_lat':                     '°',
    'position_long':                    '°',
    'altitude':                         'm',
    'smooth_altitude':                  'm',
    'heart_rate':                       'bpm',
    'cadence':                          'rpm',
    'distance':                         'm',
    'track_distance':                   'm',
    'speed':                            'm/s',
    'track_speed':                      'm/s',
    'power':                            'W',
    'power3s':                          'W',
    'power10s':                         'W',
    'power30s':                         'W',
    'grade':                            '%',
    'temperature':                      '°C',
    'accumulated_power':                'W',
    'left_right_balance':               None,
    'gps_accuracy':                     'm',
    'vertical_speed':                   'm/s',
    'calories':                         'kcal',
    'left_torque_effectiveness':        '%',
    'right_torque_effectiveness':       '%',
    'left_pedal_smoothness':            '%',
    'right_pedal_smoothness':           '%',
    'combined_pedal_smoothness':        '%',
    'respiration_rate':                 'bpm',
    'grit':                             None,
    'flow':                             None,
    'core_temperature':                 '°C',
    'front_gear_num':                   None,
    'front_gear':                       'teeth',
    'rear_gear_num':                    None,
    'rear_gear':                        'teeth',
    'active_climb':                     None, #experimental - climb number
    'jump_distance':                    'm',
    'jump_height':                      'm',
    'jump_rotations':                   'rotations',
    'jump_hang_time':                   's',
    'jump_score':                       None,
}

meta_units = {
    'start_time':                       None,
    'end_time':                         None,
    'start_position_lat':               '°',
    'start_position_long':              '°',
    'end_position_lat':                 '°',
    'end_position_long':                '°',
    'minlat':                           '°',
    'minlon':                           '°',
    'maxlat':                           '°',
    'maxlon':                           '°',
    'total_elapsed_time':               's',
    'total_timer_time':                 's',
    'total_distance':                   'm',
    'total_cycles':                     'cycles',
    'total_work':                       'J',
    'avg_speed':                        'm/s',
    'max_speed':                        'm/s',
    'training_load_peak':               None,
    'total_grit':                       None,
    'avg_flow':                         None,
    'total_calories':                   'kcal',
    'avg_power':                        'W',
    'max_power':                        'W',
    'total_ascent':                     'm',
    'total_descent':                    'm',
    'normalized_power':                 'W',
    'training_stress_score':            None,
    'intensity_factor':                 None,
    'threshold_power':                  'W',
    'avg_vam':                          'm/s',
    'avg_respiration_rate':             'bpm',
    'max_respiration_rate':             'bpm',
    'min_respiration_rate':             'bpm',
    'jump_count':                       None,
    'avg_right_torque_effectiveness':   '%',
    'avg_left_torque_effectiveness':    '%',
    'avg_right_pedal_smoothness':       '%',
    'avg_left_pedal_smoothness':        '%',
    'avg_heart_rate':                   'bpm',
    'max_heart_rate':                   'bpm',
    'avg_cadence':                      'rpm',
    'max_cadence':                      'rpm',
    'avg_temperature':                  '°C',
    'max_temperature':                  '°C',
    'min_temperature':                  '°C',
    'total_anaerobic_training_effect':  None,
    'total_strokes':                    'strokes',
    'sport_profile_name':               None,
    'sport':                            None,
    'sub_sport':                        None,
    'activity_name':                    None,
}


def generate_name(sport: str|None, sub_sport: str|None, sport_profile_name: str|None) -> str:
    name = ""

    if sport is None:
        name = "Unknown"
    else:
        name = sport.replace('_',' ').title()

    if sub_sport is not None and sub_sport != 'generic':
        name = f"{sub_sport.replace('_',' ').title()} {name}"

    if sport_profile_name is not None:
        name += f" ({sport_profile_name})"

    return name


class Reader:
    def __init__(self, fit_file: str):
        self.fit_file: str = fit_file
        self._data: dict[datetime, dict] = {}
        self._metadata: dict = {}
        self._cache: dict = {}

        self.ok: bool = self._load_fit_file()
        if self.ok:
            self._generate_calculated_fields()


    @property
    def data(self) -> Generator[tuple[datetime, dict], None, None]:
        for timestamp in sorted(self._data.keys()):
            yield timestamp, self._data[timestamp]


    @property
    def metadata(self) -> dict:
        return self._metadata


    def _load_fit_file(self) -> bool:
        def mesg_listener(mesg_num: int, message: dict) -> None:
            if mesg_num == Profile['mesg_num']['SESSION']: # type: ignore
                self._handle_session_message(message)
            elif mesg_num == Profile['mesg_num']['SPORT']: # type: ignore
                self._handle_sport_message(message)
            elif mesg_num == Profile['mesg_num']['FILE_ID']: # type: ignore
                self._handle_file_id_message(message)
            elif mesg_num == Profile['mesg_num']['RECORD']: # type: ignore
                self._handle_record_message(message)
            elif mesg_num == Profile['mesg_num']['EVENT']: # type: ignore
                self._handle_event_message(message)
            elif mesg_num == Profile['mesg_num']['CLIMB_PRO']: # type: ignore
                self._handle_climb_message(message)
            elif mesg_num == Profile['mesg_num']['JUMP']: # type: ignore
                self._handle_jump_message(message)

            # TBD messages:
            # - segment_lap
            # - hrv
            # - time_in_zone
            # - lap
            # - split
            # - split_summary
            # - timestamp_correlation
            # - device_info
            # - device_aux_battery_info
        try:
            stream = Stream.from_file(self.fit_file)
            decoder = Decoder(stream)
            _, errors = decoder.read(mesg_listener=mesg_listener)

            if errors:
                logging.error(f"Errors decoding fit file:")
                for error in errors:
                    logging.error(f" - {error}")
                return False
        except Exception as e:
            logging.error(f"Failed to read fit file: {e}")
            return False
        return True


    def _handle_session_message(self, message: dict) -> None:
        if 'timestamp' in message:
            self._metadata['end_time'] = message['timestamp']
        if 'start_time' in message:
            self._metadata['start_time'] = message['start_time']
        if 'start_position_lat' in message:
            self._metadata['start_position_lat'] = message['start_position_lat'] * SEMICIRCLES_FACTOR
        if 'start_position_long' in message:
            self._metadata['start_position_long'] = message['start_position_long'] * SEMICIRCLES_FACTOR
        if 'end_position_lat' in message:
            self._metadata['end_position_lat'] = message['end_position_lat'] * SEMICIRCLES_FACTOR
        if 'end_position_long' in message:
            self._metadata['end_position_long'] = message['end_position_long'] * SEMICIRCLES_FACTOR
        if 'nec_lat' in message:
            self._metadata['maxlat'] = message['nec_lat'] * SEMICIRCLES_FACTOR
        if 'nec_long' in message:
            self._metadata['maxlon'] = message['nec_long'] * SEMICIRCLES_FACTOR
        if 'swc_lat' in message:
            self._metadata['minlat'] = message['swc_lat'] * SEMICIRCLES_FACTOR
        if 'swc_long' in message:
            self._metadata['minlon'] = message['swc_long'] * SEMICIRCLES_FACTOR
        if 'total_elapsed_time' in message:
            self._metadata['total_elapsed_time'] = message['total_elapsed_time']
        if 'total_timer_time' in message:
            self._metadata['total_timer_time'] = message['total_timer_time']
        if 'total_distance' in message:
            self._metadata['total_distance'] = message['total_distance']
        if 'total_cycles' in message:
            self._metadata['total_cycles'] = message['total_cycles']
        if 'total_work' in message:
            self._metadata['total_work'] = message['total_work']

        if 'enhanced_avg_speed' in message:
            self._metadata['avg_speed'] = message['enhanced_avg_speed']
        elif 'avg_speed' in message:
            self._metadata['avg_speed'] = message['avg_speed']
        if 'enhanced_max_speed' in message:
            self._metadata['max_speed'] = message['enhanced_max_speed']
        elif 'max_speed' in message:
            self._metadata['max_speed'] = message['max_speed']

        if 'training_load_peak' in message:
            self._metadata['training_load_peak'] = message['training_load_peak']
        if 'total_grit' in message:
            self._metadata['total_grit'] = message['total_grit']
        if 'avg_flow' in message:
            self._metadata['avg_flow'] = message['avg_flow']
        if 'total_calories' in message:
            self._metadata['total_calories'] = message['total_calories']
        if 'avg_power' in message:
            self._metadata['avg_power'] = message['avg_power']
        if 'max_power' in message:
            self._metadata['max_power'] = message['max_power']
        if 'total_ascent' in message:
            self._metadata['total_ascent'] = message['total_ascent']
        if 'total_descent' in message:
            self._metadata['total_descent'] = message['total_descent']
        if 'normalized_power' in message:
            self._metadata['normalized_power'] = message['normalized_power']
        if 'training_stress_score' in message:
            self._metadata['training_stress_score'] = message['training_stress_score']
        if 'intensity_factor' in message:
            self._metadata['intensity_factor'] = message['intensity_factor']
        if 'threshold_power' in message:
            self._metadata['threshold_power'] = message['threshold_power']
        if 'avg_vam' in message:
            self._metadata['avg_vam'] = message['avg_vam']

        if 'enhanced_avg_respiration_rate' in message:
            self._metadata['avg_respiration_rate'] = message['enhanced_avg_respiration_rate']
        elif 'avg_respiration_rate' in message:
            self._metadata['avg_respiration_rate'] = message['avg_respiration_rate']
        if 'enhanced_max_respiration_rate' in message:
            self._metadata['max_respiration_rate'] = message['enhanced_max_respiration_rate']
        elif 'max_respiration_rate' in message:
            self._metadata['max_respiration_rate'] = message['max_respiration_rate']
        if 'enhanced_min_respiration_rate' in message:
            self._metadata['min_respiration_rate'] = message['enhanced_min_respiration_rate']
        elif 'min_respiration_rate' in message:
            self._metadata['min_respiration_rate'] = message['min_respiration_rate']

        if 'jump_count' in message:
            self._metadata['jump_count'] = message['jump_count']

        if 'avg_right_torque_effectiveness' in message:
            self._metadata['avg_right_torque_effectiveness'] = message['avg_right_torque_effectiveness']
        if 'avg_left_torque_effectiveness' in message:
            self._metadata['avg_left_torque_effectiveness'] = message['avg_left_torque_effectiveness']
        if 'avg_right_pedal_smoothness' in message:
            self._metadata['avg_right_pedal_smoothness'] = message['avg_right_pedal_smoothness']
        if 'avg_left_pedal_smoothness' in message:
            self._metadata['avg_left_pedal_smoothness'] = message['avg_left_pedal_smoothness']

        if 'avg_heart_rate' in message:
            self._metadata['avg_heart_rate'] = message['avg_heart_rate']
        if 'max_heart_rate' in message:
            self._metadata['max_heart_rate'] = message['max_heart_rate']
        if 'avg_cadence' in message:
            self._metadata['avg_cadence'] = message['avg_cadence']
        if 'max_cadence' in message:
            self._metadata['max_cadence'] = message['max_cadence']
        if 'avg_temperature' in message:
            self._metadata['avg_temperature'] = message['avg_temperature']
        if 'max_temperature' in message:
            self._metadata['max_temperature'] = message['max_temperature']
        if 'min_temperature' in message:
            self._metadata['min_temperature'] = message['min_temperature']
        if 'total_anaerobic_training_effect' in message:
            self._metadata['total_anaerobic_training_effect'] = message['total_anaerobic_training_effect']
        if 'total_strokes' in message:
            self._metadata['total_strokes'] = message['total_strokes']

        if 'sport_profile_name' in message:
            self._metadata['sport_profile_name'] = message['sport_profile_name']
        if 'sport' in message:
            self._metadata['sport'] = message['sport']
        if 'sub_sport' in message:
            self._metadata['sub_sport'] = message['sub_sport']

        self._metadata['activity_name'] = generate_name(self._metadata.get('sport'), self._metadata.get('sub_sport'), self._metadata.get('sport_profile_name'))


    def _handle_sport_message(self, message: dict) -> None:
        if 'name' in message:
            self._metadata['sport_profile_name'] = message['name']
        if 'sport' in message:
            self._metadata['sport'] = message['sport']
        if 'sub_sport' in message:
            self._metadata['sub_sport'] = message['sub_sport']

        self._metadata['activity_name'] = generate_name(self._metadata.get('sport'), self._metadata.get('sub_sport'), self._metadata.get('sport_profile_name'))


    def _handle_file_id_message(self, message: dict) -> None:
        manufacturer = None
        product = None
        serial_number = None

        if 'manufacturer' in message:
            manufacturer = str(message['manufacturer'])

        if 'garmin_product' in message:
            product = str(message['garmin_product'])
        elif 'product' in message:
            product = str(message['product'])

        if 'serial_number' in message:
            serial_number = str(message['serial_number'])

        device = ""
        if manufacturer is not None:
            device += manufacturer
        if product is not None:
            if device != "":
                device += " "
            device += product

        device = device.replace('_',' ').title()

        if device == "":
            device = "Unknown Device"

        if serial_number is not None:
            device += f" (S/N: {serial_number})"

        self._metadata['device'] = device


    def _handle_record_message(self, message: dict) -> None:
        if 'timestamp' not in message:
            logging.warning("RECORD message without timestamp field.")
            return
        
        timestamp = message['timestamp']
        if timestamp not in self._data:
            self._data[timestamp] = {}

        record_data = {}

        record_data['timestamp'] = timestamp
        
        if 'position_lat' in message:
            record_data['position_lat'] = message['position_lat'] * SEMICIRCLES_FACTOR
        if 'position_long' in message:
            record_data['position_long'] = message['position_long'] * SEMICIRCLES_FACTOR

        if 'enhanced_altitude' in message:
            record_data['altitude'] = message['enhanced_altitude']
        elif 'altitude' in message:
            record_data['altitude'] = message['altitude']

        if 'heart_rate' in message:
            record_data['heart_rate'] = message['heart_rate']
        if 'cadence' in message:
            record_data['cadence'] = message['cadence']
        if 'distance' in message:
            record_data['distance'] = message['distance']

        if 'enhanced_speed' in message:
            record_data['speed'] = message['enhanced_speed']
        elif 'speed' in message:
            record_data['speed'] = message['speed']

        if 'power' in message:
            record_data['power'] = message['power']
        if 'grade' in message:
            record_data['grade'] = message['grade']
        if 'temperature' in message:
            record_data['temperature'] = message['temperature']
        if 'accumulated_power' in message:
            record_data['accumulated_power'] = message['accumulated_power']
        if 'left_right_balance' in message:
            record_data['left_right_balance'] = message['left_right_balance']
        if 'gps_accuracy' in message:
            record_data['gps_accuracy'] = message['gps_accuracy']
        if 'vertical_speed' in message:
            record_data['vertical_speed'] = message['vertical_speed']
        if 'calories' in message:
            record_data['calories'] = message['calories']
        if 'left_torque_effectiveness' in message:
            record_data['left_torque_effectiveness'] = message['left_torque_effectiveness']
        if 'right_torque_effectiveness' in message:
            record_data['right_torque_effectiveness'] = message['right_torque_effectiveness']
        if 'left_pedal_smoothness' in message:
            record_data['left_pedal_smoothness'] = message['left_pedal_smoothness']
        if 'right_pedal_smoothness' in message:
            record_data['right_pedal_smoothness'] = message['right_pedal_smoothness']
        if 'combined_pedal_smoothness' in message:
            record_data['combined_pedal_smoothness'] = message['combined_pedal_smoothness']
        if 'enhanced_respiration_rate' in message:
            record_data['respiration_rate'] = message['enhanced_respiration_rate']
        if 'grit' in message:
            record_data['grit'] = message['grit']
        if 'flow' in message:
            record_data['flow'] = message['flow']
        if 'core_temperature' in message:
            record_data['core_temperature'] = message['core_temperature']

        self._data[timestamp].update(record_data)

        for key,value in self._cache.items():
            if key not in self._data[timestamp]:
                self._data[timestamp][key] = value


    def _handle_event_message(self, message: dict) -> None:
        if 'timestamp' not in message:
            logging.warning("EVENT message without timestamp field.")
            return
        if 'event' not in message:
            logging.warning("EVENT message without event field.")
            return
        if 'event_type' not in message:
            logging.warning("EVENT message without event_type field.")
            return

        timestamp = message['timestamp']
        if timestamp not in self._data:
            self._data[timestamp] = {}

        data = {}
        if message['event'] == 'front_gear_change' and message['event_type'] == 'marker':
            front_gear_num = message.get('front_gear_num', None)
            if isinstance(front_gear_num, int) and 0 < front_gear_num < 255:
                data['front_gear_num'] = front_gear_num

            front_gear = message.get('front_gear', None)
            if isinstance(front_gear, int) and 0 < front_gear < 255:
                data['front_gear'] = front_gear

        if message['event'] == 'rear_gear_change' and message['event_type'] == 'marker':
            rear_gear_num = message.get('rear_gear_num', None)
            if isinstance(rear_gear_num, int) and 0 < rear_gear_num < 255:
                data['rear_gear_num'] = rear_gear_num

            rear_gear = message.get('rear_gear', None)
            if isinstance(rear_gear, int) and 0 < rear_gear < 255:
                data['rear_gear'] = rear_gear

        self._data[timestamp].update(data)
        self._cache.update(data)


    def _handle_climb_message(self, message: dict) -> None:
        if 'timestamp' not in message:
            logging.warning("CLIMB_PRO message without timestamp field.")
            return
        if 'climb_pro_event' not in message:
            logging.warning("CLIMB_PRO message without climb_pro_event field.")
            return
        if 'climb_number' not in message:
            logging.warning("CLIMB_PRO message without climb_number field.")
            return

        timestamp = message['timestamp']
        if timestamp not in self._data:
            self._data[timestamp] = {}

        if message['climb_pro_event'] == 'start':
            climb = message['climb_number']

            self._data[timestamp]['active_climb'] = climb
            self._cache['active_climb'] = climb
        elif message['climb_pro_event'] == 'complete':
            if 'active_climb' not in self._cache:
                logging.info('Received climb_pro complete event without climb_pro start event. Updating climb active from start.')
                climb = message['climb_number']
                for t,r in self.data:
                    if t < timestamp:
                        r['active_climb'] = climb

            if 'active_climb' in self._data[timestamp]:
                del self._data[timestamp]['active_climb']
            if 'active_climb' in self._cache:
                del self._cache['active_climb']


    def _handle_jump_message(self, message: dict) -> None:
        if 'timestamp' not in message:
            logging.warning("JUMP message without timestamp field.")
            return

        timestamp = message['timestamp']
        if timestamp not in self._data:
            self._data[timestamp] = {}

        data = {}

        if 'distance' in message and isinstance(message['distance'], (int, float)) and not math.isnan(message['distance']):
            data['jump_distance'] = message['distance']
        if 'height' in message and isinstance(message['height'], (int, float)) and not math.isnan(message['height']):
            data['jump_height'] = message['height']
        if 'rotations' in message and isinstance(message['rotations'], (int, float)) and not math.isnan(message['rotations']):
            data['jump_rotations'] = message['rotations']
        if 'hang_time' in message and isinstance(message['hang_time'], (int, float)) and not math.isnan(message['hang_time']):
            data['jump_hang_time'] = message['hang_time']
        if 'score' in message and isinstance(message['score'], (int, float)) and not math.isnan(message['score']):
            data['jump_score'] = message['score']

        self._data[timestamp].update(data)


    def _generate_calculated_fields(self) -> None:
        self._calculate_bounds()
        self._calculate_activity_time()
        self._calculate_distance()
        self._calculate_smooth_altitude()
        self._calculate_speed()
        self._calculate_power_rolling_averages()
        self._calculate_grade()
        self._calculate_vertical_speed()


    def _calculate_bounds(self) -> None:
        if set(['minlat', 'minlon', 'maxlat', 'maxlon']).issubset(self._metadata.keys()):
            return

        logging.debug("Calculating bounds")

        lats = []
        lons = []

        for timestamp,record in self.data:
            if 'position_lat' in record:
                lats.append(record['position_lat'])
            if 'position_long' in record:
                lons.append(record['position_long'])

        if len(lats) > 0 and len(lons) > 0:
            self._metadata['minlat'] = min(lats)
            self._metadata['maxlat'] = max(lats)
            self._metadata['minlon'] = min(lons)
            self._metadata['maxlon'] = max(lons)


    def _calculate_activity_time(self) -> None:
        logging.debug("Calculating activity time")

        start_time = None
        for timestamp,record in self.data:
            if start_time is None:
                start_time = timestamp

            record['time'] = (timestamp - start_time).total_seconds()


    def _calculate_distance(self) -> None:
        logging.debug("Calculating distance")

        last_lat = None
        last_lon = None
        total_distance = 0.0

        for timestamp,record in self.data:
            if 'position_lat' in record and 'position_long' in record:
                lat = record['position_lat']
                lon = record['position_long']

                if last_lat is not None and last_lon is not None:
                    total_distance += geo_distance(last_lat, last_lon, lat, lon)

                last_lat = lat
                last_lon = lon

            record['track_distance'] = total_distance
            if 'distance' not in record:
                record['distance'] = total_distance


    def _calculate_smooth_altitude(self) -> None:
        logging.debug("Calculating smooth altitude")

        for record, window in self._sliding_window(SMOOTH_ALTITUDE_TIME_WINDOW, 'time'):
            altitudes = [r['altitude'] for r in window if 'altitude' in r]
            if len(altitudes) > 0:
                record['smooth_altitude'] = statistics.mean(altitudes)


    def _calculate_speed(self) -> None:
        logging.debug("Calculating speed")

        last_time = None
        last_distance = None

        for timestamp,record in self.data:
            if 'distance' in record and 'time' in record:
                distance = record['distance']
                time = record['time']

                if last_time is not None and last_distance is not None:
                    time_delta = time - last_time
                    if time_delta > 0:
                        speed = (distance - last_distance) / time_delta
                        record['track_speed'] = speed
                        if 'speed' not in record:
                            record['speed'] = speed

                last_time = time
                last_distance = distance


    def _calculate_power_rolling_averages(self) -> None:
        logging.debug("Calculating power rolling averages (3s, 10s, 30s)")

        power = {}
        for timestamp,record in self.data:
            if 'power' in  record:
                power[timestamp] = record['power']

            power3s = [p[1] for p in power.items() if p[0] > timestamp - timedelta(seconds=3)]
            power10s = [p[1] for p in power.items() if p[0] > timestamp - timedelta(seconds=10)]
            power30s = [p[1] for p in power.items() if p[0] > timestamp - timedelta(seconds=30)]
            if len(power3s) > 0:
                record['power3s'] = statistics.mean(power3s)
            if len(power10s) > 0:
                record['power10s'] = statistics.mean(power10s)
            if len(power30s) > 0:
                record['power30s'] = statistics.mean(power30s)

            power = {k: v for k, v in power.items() if k > timestamp - timedelta(seconds=30)}


    def _calculate_grade(self) -> None:
        logging.debug("Calculating grade")

        alt_key = 'smooth_altitude'
        dist_key = 'distance'

        for record, window in self._sliding_window(MAX_GRADE_WINDOW, dist_key):
            dist = record.get(dist_key, None)
            alt = record.get(alt_key, None)
            if dist is None or alt is None:
                continue

            altitudes = [(r[dist_key], r[alt_key]) for r in window if alt_key in r and dist_key in r]
            z1,y1 = altitudes[0]
            z2,y2 = altitudes[-1]

            if dist - z1 < MIN_GRADE_WINDOW/2:
                continue # don't calculate grade - covers beginning of activity
            if z2 - dist < MIN_GRADE_WINDOW/2:
                continue # don't calculate grade - covers end of activity

            z = z2 - z1
            y = y2 - y1

            x = math.sqrt(z**2 - y**2) # pythagoras (x**2 + y**2 = z**2 where z is distance delta and y is altitude delta)

            record['grade'] = (y / x) * 100.0


    def _calculate_vertical_speed(self) -> None:
        logging.debug("Calculating vertical speed")

        last_time = None
        last_altitude = None

        for timestamp,record in self.data:
            if 'altitude' in record and 'time' in record:
                altitude = record['altitude']
                time = record['time']

                if 'vertical_speed' not in record and last_time is not None and last_altitude is not None:
                    time_delta = time - last_time
                    if time_delta > 0:
                        vertical_speed = (altitude - last_altitude) / time_delta
                        record['vertical_speed'] = vertical_speed

                last_time = time
                last_altitude = altitude


    def _sliding_window(self, window_size: float, key: str) -> Generator[tuple[dict, list[dict]], None, None]:
        def in_window(record: dict, target: float) -> bool:
            value = record.get(key, None)
            if value is None:
                return False
            delta: float = abs(value - target)
            return delta <= (window_size / 2.0)

        seq = [record for _,record in self.data]

        for i, cur in enumerate(seq):
            value = cur.get(key, None)
            if value is None:
                logging.warning(f"Record without {key} field in sliding window calculation. Skipping.")
                continue

            # backward until condition fails
            left = []
            j = i - 1
            while j >= 0 and in_window(seq[j], value):
                left.append(seq[j])
                j -= 1
            left.reverse()

            # forward until condition fails
            right = []
            k = i + 1
            while k < len(seq) and in_window(seq[k], value):
                right.append(seq[k])
                k += 1

            yield cur, left + [cur] + right


