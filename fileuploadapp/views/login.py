
from django.shortcuts import render
from django.contrib import messages

def checkloginornot(request):
    if request.method == "POST":
        username = request.POST['username']
        password = request.POST['password']
        if username=='admin' and password=='admin':           
            return render(request, 'file_upload.html')
        else:
                
            context={
                'msg':"Please Enter Correct Password And Username"  
            } 
            return render(request,'login.html',context)   
    return render(request,'login.html')      
            