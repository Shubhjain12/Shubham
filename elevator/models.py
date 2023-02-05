from django.db import models

# Create your models here.

class Elevator(models.Model):
    id = models.AutoField(db_column='id', primary_key=True) # primary key
    location = models.CharField(db_column='location', max_length=50, null=True) # location of the elevator
    status = models.ForeignKey('ElevatorStatus', on_delete=models.CASCADE, db_column='status', null=True)
    current_floor = models.IntegerField(db_column='current_floor', null=False) 
    destination_floor = models.IntegerField(db_column='destination_floor', null=True, blank=True)
    direction = models.BooleanField(db_column='direction', null=True, blank=True) # True if going up, False if going down
    min_floor = models.IntegerField(db_column='min_floor', null=False) # min floor served by the elevator
    max_floor = models.IntegerField(db_column='max_floor', null=False) # max floor served by the elevator
    max_occupancy = models.IntegerField(db_column='max_occupancy', null=False) # max occupancy of the elevator
    current_occupancy = models.IntegerField(db_column='current_occupancy', null=False) # current occupancy of the elevator
    def __str__(self):
        return str(self.location)
    class Meta:
        db_table = 'elevator'

class ElevatorStatus(models.Model):
    id = models.AutoField(db_column='id', primary_key=True) # primary key
    status = models.CharField(db_column='status', max_length=20, null=False) # idle, moving, stopped

    def __str__(self):
        return str(self.status)
    class Meta:
        db_table = 'elevator_status'
    