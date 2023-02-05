import json

from django.test import TestCase
from rest_framework.test import APIClient

CONTENT_TYPE = "application/json; charset=utf-8"

# Testing for elevator views to be changed later on
class ElevatorViewsTestCase(TestCase):
    """
    Test cases for elevator modules
    """
    def test_create_elevator(self):
        """
        Testing for create new elevator
        """
        payload = {
        "location": "main",
        "current_floor": 0,
        "destination_floor": 0,
        "direction": None,
        "working": True,
        "min_floor": 0,
        "max_floor": 10,
        "max_occupancy": 10,
        "current_occupancy": 0,
        "status": None
    }

        client = APIClient()

        response = client.post('/elevator/elevator/', json.dumps(payload), content_type='application/json')
        response_json = response.json()
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response_json.get('id'), 1)