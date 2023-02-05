Description
---------------------

This is a Django based implementation of Rest frame work 

Please check working demo  <a href="/readme_assets/elevator_API_demo.mp4">video here</a> and screenshots in <a href="/readme_assets">readme_assets</a> folder

Redis Installation:

 Install Redis on Windows 10:
    
   follows steps : hackthedeveloper.com/how-to-install-redis-on-windows-10/

   note:- Make sure that redis server should be running while excuting program.

 Install Redis in django with pip:

     $ python -m pip install django-redis

Add cache to Setting.py

CACHE_TTL = 60 * 1500

CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_redis.client.DefaultClient"
        },
        "KEY_PREFIX": "example"
    }
}


API Endpoints:
---------------------

Endpoint: `/elevator/elevator/`


>METHODS: [GET,POST]  

Example Payload for creating new elevator:  
    {  
            "location": "main",  
            "current_floor": 0,  
            "destination_floor": null,  
            "direction": null,  
            "working": true,  
            "min_floor": 0,  
            "max_floor": 10,  
            "max_occupancy": 10,  
            "current_occupancy": 0,  
            "status": 1  
        }
        

Endpoint: `/elevator/elevator/{elevator-id}` 

>METHODS: [GET, DELETE, PUT]  

Example JSON Payload for update:   
```json
    {  
            "location": "main",  
            "current_floor": 0,  
            "destination_floor": null,  
            "direction": null,  
            "working": true,  
            "min_floor": 0,  
            "max_floor": 10,  
            "max_occupancy": 10,  
            "current_occupancy": 0,  
            "status": 1  
        }
```


<li><h3>Endpoint: `/elevator/use/` <h3> </li>  

>METHODS: [POST] 

Example payload for using an elevator (elevator_name is optional, if id is not provided it is used) :  

{
    "elevator_id": 1,  
    "elevator_name": "main",  
    "current_floor": 1,
    "destination_floor": 7
}

Endpoint: `/elevator/maintainence/` 

>METHODS: [POST] 

Example payload to put elevator in maintainance and in normal mode (action supports "start" and "finish") : 

{
    "elevator_id": 1,
    "action": "start"
}


</ul>

Running on local machine
---------------------
1. Clone this project<br>
  ```
  git clone https://github.com/avina5hkr/Elevator_API_with_django.git
  ```  
2. Install the python dependencies
  ```python
  pip install -r requirements.txt
  ```
3. run migrations to add tables to db
  ```python
  python manage.py migrate
  ```
4. Run the development server
  ```python
  python manage.py runserver  ```
5. create a superuser for the admin page
```python
   python manage.py createsuperuser
```
6. Go to django admin page and add values ``"idle"`` and ``"moving"`` in ElevatorStatus table  
```
http://127.0.0.1:8000/admin/elevator/elevatorstatus/
```  





