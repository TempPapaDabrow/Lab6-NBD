import riak
import random

def insert_into_db(bucket, key, value):
    newBook = bucket.new(value[key], data=value)
    newBook.store()

def fetch_from_db(bucket, key, value):
    fetchedBook = bucket.get(value[key])
    print(fetchedBook.encoded_data)

def update_db(bucket, key, value):
    fetchedBook = bucket.get(value[key])
    fetchedBook.data['title'] = "WITAM SERDECZNIE"
    fetchedBook.store()

def delete_from_db(bucket, key, value):
    fetchedBook = bucket.get(value[key])
    fetchedBook.delete()

myClient = riak.RiakClient(pb_port=8087, protocol='pbc')

# Because the Python client uses the Protocol Buffers interface by
# default, the following will work the same:
myClient = riak.RiakClient(pb_port=8087)

booksBucket = myClient.bucket("books")

book = {
  'isbn': "1111979723",
  'title': "Moby Dick",
  'author': "Herman Melville",
  'body': "Call me Ishmael. Some years ago...",
  'copies_owned': 3
}

insert_into_db(booksBucket, 'isbn', book)

fetch_from_db(booksBucket, 'isbn', book)

update_db(booksBucket, 'isbn', book)

fetch_from_db(booksBucket, 'isbn', book)

delete_from_db(booksBucket, 'isbn', book)

fetch_from_db(booksBucket, 'isbn', book)
