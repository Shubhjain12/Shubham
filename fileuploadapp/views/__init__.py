from .file_upload import *
from .login import *
from django.shortcuts import render
from django.http import HttpResponse


def index(request):
    return HttpResponse("Hello, world. You're at the Home page of Django sample project error 404.")
