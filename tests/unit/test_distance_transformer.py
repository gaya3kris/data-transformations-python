from data_transformations.citibike import distance_transformer
from tests.unit import SPARK

def test_calculate_distance():
    start_latitude = 40.69102925677968
    start_longitude = -73.99183362722397
    end_latitude = 40.6763947
    end_longitude = -73.99869893

    expected = 1.07
    actual = distance_transformer.calculate_distance(start_latitude,start_longitude,end_latitude,end_longitude)

    assert actual == expected
