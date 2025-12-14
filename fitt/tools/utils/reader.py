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
}


class Reader:
    def __init__(self, fit_file: str):
        self.fit_file: str = fit_file
        self._data: dict[datetime, dict] = {}

        self.ok: bool = self._load_fit_file()
        if self.ok:
            self._generate_calculated_fields()

    @property
    def data(self) -> Generator[tuple[datetime, dict], None, None]:
        for timestamp in sorted(self._data.keys()):
            yield timestamp, self._data[timestamp]


    def _load_fit_file(self) -> bool:
        def mesg_listener(mesg_num: int, message: dict) -> None:
            if mesg_num == Profile['mesg_num']['RECORD']: # type: ignore
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


    def _generate_calculated_fields(self) -> None:
        self._calculate_activity_time()
        self._calculate_distance()
        self._calculate_smooth_altitude()
        self._calculate_speed()
        self._calculate_power_rolling_averages()
        self._calculate_grade()
        self._calculate_vertical_speed()


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
