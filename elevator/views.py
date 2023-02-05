import json
from rest_framework import viewsets
from elevator import models, serializers
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from elevator.models import Elevator, ElevatorStatus
import time
from django.core.cache import cache

def move_elevator(elevator_id):
    """
    This function moves the elevator to the destination floor
    """
    elevator = Elevator.objects.get(id=elevator_id)
    if elevator.direction:
        while elevator.current_floor < elevator.destination_floor:
            elevator = Elevator.objects.get(id=elevator_id)
            elevator.current_floor += 1
            elevator.save()
            time.sleep(0.1)
    else:
        while elevator.current_floor > elevator.destination_floor:
            elevator = Elevator.objects.get(id=elevator_id)
            elevator.current_floor -= 1
            elevator.save()
            time.sleep(0.1)
    elevator.destination_floor = None
    elevator.status = ElevatorStatus.objects.get(status="idle")
    elevator.save()
    

def calling_elevator(elevator_id, current_floor, destination_floor):
        """
        This is called when somebody presses the up or down button to call the elevator.
        This could happen at any time, whether or not the elevator is moving.
        """
        elevator = Elevator.objects.get(id=elevator_id)
        if elevator.status == None:
            return "This elevator is not working"
        if current_floor < elevator.min_floor or current_floor > elevator.max_floor:
            return "Current floor is not served by this elevator"
        if destination_floor < elevator.min_floor or destination_floor > elevator.max_floor:
            return "Destination floor is not served by this elevator"
        if elevator.status.status == "idle":
            elevator_when_idle(elevator_id, current_floor, destination_floor)
            move_elevator(elevator_id)
        elif elevator.status.status == "moving":
            while(elevator.direction != current_floor < destination_floor or elevator.status.status != "idle"):
                elevator = Elevator.objects.get(id=elevator_id)
                time.sleep(0.5)
            if elevator.status.status == "idle":
                elevator_when_idle(elevator_id, current_floor, destination_floor)
                move_elevator(elevator_id)
            elif elevator.destination_floor < destination_floor:
                elevator.destination_floor = destination_floor
                move_elevator(elevator_id)
        elevator = Elevator.objects.get(id=elevator_id)
        return f"Request completed, elevator is at {elevator.current_floor} floor, and status is {elevator.status.status}"

def elevator_when_idle(elevator_id, current_floor, destination_floor):
    """
    This is called when the elevator is idle.
    """
    elevator = Elevator.objects.get(id=elevator_id)
    if elevator.status.status == "idle":
        elevator.current_floor = current_floor
        elevator.destination_floor = destination_floor
        elevator.direction = current_floor < destination_floor
        elevator.status = ElevatorStatus.objects.get(status="moving")
        elevator.save()

def elvator_is_in_maintainence(elevator_id):
    """
    This is called when the elevator is in maintainence mode.
    """
    elevator = Elevator.objects.get(id=elevator_id)
    elevator.status = None
    elevator.save()
    return "Elevator is in maintainence mode"

def maintainence_complete(elevator_id):
    """
    This is called when the elevator is finished with maintainence mode.
    """
    elevator = Elevator.objects.get(id=elevator_id)
    elevator.current_floor = elevator.min_floor
    elevator.destination_floor = None
    elevator.direction = None
    elevator.status = ElevatorStatus.objects.get(status="idle")
    elevator.save()
    return "Maintainence complete, elevator is ready to use"



class ElevatorViewSet(viewsets.ModelViewSet):
    """
    This method is for Serialization
    """
    queryset = models.Elevator.objects.all()
    serializer_class = serializers.ElevatorSerializer


@api_view(["POST"])
def elevator_function(request):
    """
    Handles the elevator request given my user
    """
    if request.method == 'POST':
        payload = request.data
        elevator_id = payload.get('elevator_id')
        if cache.get('id') == elevator_id and cache.get('flag') == 1 :
            payload=cache.get('elevator')
        else:
            if str(elevator_id).isnumeric():
                elevator_name = payload.get('elevator_name') # user can also pass the name of the elevator
                if elevator_name is not None:
                    elevator = models.Elevator.objects.get(name=elevator_name)
                    if elevator is not None:
                        elevator_id = elevator.id
            current_floor = payload.get('current_floor')
            destination_floor = payload.get('destination_floor')
            try:
                elevator_id = int(elevator_id)
                current_floor = int(current_floor)
                destination_floor = int(destination_floor)
            except Exception as e:
                return Response({'error': 'Missing or bad parameters'}, status=status.HTTP_400_BAD_REQUEST)

            request_status = calling_elevator(elevator_id, current_floor, destination_floor)
            payload = {
                'elevator_id': elevator_id,
                'request_status': request_status
            }
            cache.set('flag',1)
            cache.set('id',elevator_id)
            cache.set('elevator',payload)
        return Response(payload,status=status.HTTP_200_OK)
    return Response({'error': 'Method not allowed'}, status=status.HTTP_405_METHOD_NOT_ALLOWED)


@api_view(["POST"])
def elevator_maintainence(request):
    """
    Handles maintainence requests for the elevator
    """
    if request.method == 'POST':
        payload = json.loads(request.body.decode('utf-8'))
        elevator_id = payload.get('elevator_id')
        action = payload.get('action')
        # if cache.get('id') == elevator_id :
        #     payload=cache.get('elevator')
        # else:    
        if str(elevator_id).isnumeric():
            elevator_name = payload.get('elevator_name') # user can also pass the name of the elevator
            if elevator_name is not None:
                elevator = models.Elevator.objects.get(name=elevator_name)
                elevator_id = elevator.id
        if action == 'start':
            request_status = elvator_is_in_maintainence(elevator_id)
            return_payload = {
                'elevator_id': elevator_id,
                'request_status': request_status
            }
            cache.set('flag',2)
            # cache.set('elevator',return_payload)
            return Response(return_payload,status=status.HTTP_200_OK)
        elif action == 'finish':
            request_status = maintainence_complete(elevator_id)
            return_payload = {
                'elevator_id': elevator_id,
                'request_status': request_status
            }
            cache.set('flag',2)
            # cache.set('elevator',return_payload)
            return Response(return_payload,status=status.HTTP_200_OK)
        else:
            
            return Response({'error': 'Missing or bad parameters'}, status=status.HTTP_400_BAD_REQUEST)
    return Response({'error': 'Method not allowed'}, status=status.HTTP_405_METHOD_NOT_ALLOWED)
