# Generated by Django 4.1.5 on 2023-01-23 11:19

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('elevator', '0001_initial'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='elevator',
            name='working',
        ),
        migrations.AlterField(
            model_name='elevatorstatus',
            name='status',
            field=models.CharField(db_column='status', max_length=20),
        ),
    ]