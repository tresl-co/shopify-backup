#! /usr/bin/env python3

import os, math
import requests
import sqlalchemy
from sqlalchemy import MetaData, create_engine, Column, BigInteger, DateTime, String, ForeignKey, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker


# Environment variables
if os.path.exists('config.env'):
    for line in open('config.env'):
        var = line.strip().split('=')
        if len(var) == 2:
            os.environ[var[0]] = var[1].replace("\"", "")

# Metadata settings
convention = {
    "ix": 'ix_%(column_0_label)s',
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}
Base = declarative_base()
Base.metadata = MetaData(naming_convention=convention)

# Models
class Product(Base):
    __tablename__ = 'products'
    id = Column(BigInteger, primary_key=True)
    title = Column(String)

class Customer(Base):
    __tablename__ = 'customers'
    id = Column(BigInteger, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    email = Column(String)
    orders = relationship('Order', back_populates='customer')

class Order(Base):
    __tablename__ = 'orders'
    id = Column(BigInteger, primary_key=True)
    customer_id = Column(BigInteger, ForeignKey('customers.id', ondelete='cascade'))
    currency = Column(String)
    total_price = Column(String)
    customer = relationship('Customer', back_populates='orders')

# Create tables
basedir = os.path.abspath(os.path.dirname(__file__))
SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'db.sqlite')
engine = create_engine(SQLALCHEMY_DATABASE_URI)
session = sessionmaker()
session.configure(bind=engine)
db = session()
Base.metadata.create_all(engine)

# Ingest data
s = requests.Session()
s.auth = (os.getenv('SHOPIFY_API_KEY'), os.getenv('SHOPIFY_API_PASSWORD'))
url = 'https://' + os.getenv('SHOPIFY_URL') + '/admin/'
params = {'limit': 250}

## Products
Model = Product
model = 'products'
field_values = ['title']
count = s.get(url + model + '/count.json').json().get('count')
pages = math.ceil(count/250)
print("Starting import for {}...".format(model))
num = 0
for page in range(1, pages+1):
    r = s.get(url + model + '.json', params={'page': page, **params})
    objs = [i for i in r.json().get(model)]
    for i in objs:
        fields = {k: i.get(k) for k in field_values}
        obj = db.query(Model).filter_by(id=i['id'])
        if obj.first() is not None:
            obj.update(fields)
        else:
            obj = Model(id=i['id'], **fields)
            db.add(obj)
        num += 1
print("Imported {} {}.".format(num, model))

## Customers
Model = Customer
model = 'customers'
field_values = ['first_name', 'last_name', 'email']
count = s.get(url + model + '/count.json').json().get('count')
pages = math.ceil(count/250) # max 250 results per page
print("Starting import for {}...".format(model))
num = 0
for page in range(1, pages+1):
    r = s.get(url + model + '.json', params={'page': page, **params})
    objs = [i for i in r.json().get(model)]
    for i in objs:
        fields = {k: i.get(k) for k in field_values}
        obj = db.query(Model).filter_by(id=i['id'])
        if obj.first() is not None:
            obj.update(fields)
        else:
            obj = Model(id=i['id'], **fields)
            db.add(obj)
        num += 1
print("Imported {} {}.".format(num, model))

## Store products and customers for orders later
db.commit()

## Orders
Model = Order
model = 'orders'
field_values = ['currency', 'total_price']
count = s.get(url + model + '/count.json', params={'status': 'any'}).json().get('count')
pages = math.ceil(count/250)
print("Starting import for {}...".format(model))
num = 0
for page in range(1, pages+1):
    r = s.get(url + model + '.json', params={'page': page, 'status': 'any', **params})
    objs = [i for i in r.json().get(model)]
    for i in objs:
        customer = db.query(Customer).get(i['customer']['id'])
        if customer is None:
            continue
        fields = {k: i.get(k) for k in field_values}
        obj = db.query(Model).filter_by(id=i['id'])
        if obj.first() is not None:
            obj.update(fields)
        else:
            obj = Model(id=i['id'], customer_id=customer.id, **fields)
            customer.orders.append(obj)
            db.add(obj)
        num += 1
print("Imported {} {}.".format(num, model))

## Store orders
db.commit()