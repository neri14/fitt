import math

radio_terrestre = 6372797.5605
grados_radianes = math.pi / 180


def geo_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1 = lat1 * grados_radianes
    lon1 = lon1 * grados_radianes
    lat2 = lat2 * grados_radianes
    lon2 = lon2 * grados_radianes

    haversine = (math.sin((lat2 - lat1)/2.0) ** 2) + (math.cos(lat1) * math.cos(lat2) * (math.sin((lon2 - lon1)/2.0) ** 2))
    dist = 2 * math.asin(min(1.0, math.sqrt(haversine))) * radio_terrestre

    return dist
