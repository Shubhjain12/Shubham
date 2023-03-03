import pandas as pd
from django.shortcuts import render
import os
from django.conf import settings
import traceback



def file_upload(request):
    '''
        This function is to upload file in folder 
    '''
    try:
        if request.method == "POST":
            file = request.FILES['fileup']
            extension=str(file).split(".")
            print("extension",extension)
            file_path = os.path.join('upload/')
            excel_File = pd.DataFrame(file)
            if os.path.exists(file_path):
                if extension[1] == 'csv':
                    excel_File.to_csv(file_path+str(file),index=False)
                else:
                    excel_File.to_excel(file_path+str(file),index=False)   
            else:
                os.makedirs(file_path, exist_ok=True)
                if extension[1] == 'csv':    
                    excel_File.to_csv(file_path+str(file),index=False)
                else:
                    excel_File.to_excel(file_path+str(file),index=False)

            dir_list = os.listdir(file_path) 
            context={
                    'file':dir_list
            } 
            return render(request,'file_upload.html',context)   
    except Exception as e:
        traceback.print_exc()


def file_read(request):
    '''
        This function is to read the file from the folder 
    '''
    if request.method == "POST":
       try:
            add_flagg=request.POST['f_name']
            file_path = open(os.path.join(settings.MEDIA_ROOT,str(add_flagg)))
            excel_File = pd.DataFrame(file_path)
            excel_File = excel_File.to_html()
            context={
                'excel_File':excel_File
            }
            return render(request,'table.html',context) 
       except Exception as e:
           traceback.print_exc()


