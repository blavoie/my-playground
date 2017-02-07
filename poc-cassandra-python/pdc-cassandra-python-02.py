# -*- coding: utf-8 -*-
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection

# Define a model
class Users(Model):
    firstname = columns.Text()
    age = columns.Integer()
    city = columns.Text()
    email = columns.Text()
    lastname = columns.Text(primary_key=True)

    def __repr__(self):
        return '%s %d' % (self.firstname, self.age)


connection.setup(['n01.cl01'], "brlav35")

from cqlengine.management import sync_table

# Sync your model with your cql table
sync_table(Users)

# Create a row of user info for Bob
Users.create(firstname='Bob', age=35, city='Austin', email='bob@example.com', lastname='Jones')

# Read Bob’s information back and print
q=Users.get(lastname='Jones')
print q