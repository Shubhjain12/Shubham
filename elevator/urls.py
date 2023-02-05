from django.urls import path, include
from rest_framework import routers

from elevator import views
router = routers.DefaultRouter()
router.register(r'elevator', views.ElevatorViewSet)

urlpatterns = [
    path('', include(router.urls)),
    path('use/', views.elevator_function),
    path('maintainence/', views.elevator_maintainence),
    path('api-auth/', include('rest_framework.urls'))
]