#!/usr/bin/env python
# encoding: utf-8

"""
@author: demo
@license: Copyright 2019-2022, LineZoneData 
@contact: demo@linezonedata.com
@desc: 
"""

from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser

user = PasswordUser(models.User())
user.username = 'admin'
user.email = 'admin@admin.com'
user.password = 'lz12345+'
user.superuser = True

# the secret sauce is right here
from sqlalchemy import create_engine
engine = create_engine("postgresql://airflow:airflow@192.168.21.151:5432/airflow")

session = settings.Session(bind=engine)
session.add(user)
session.commit()
session.close()
