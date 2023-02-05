from django.contrib import admin
from elevator.models import Elevator, ElevatorStatus


admin.site.register(Elevator) # register the Elevator model in the admin site
admin.site.register(ElevatorStatus)
