#!/usr/bin/env python
# vi: tabstop=4 smarttab shiftwidth=4
#
# Fix CRLF inconsitencies within a ZCS mailstore. Written for Python 2.6, if
# you have some other version, your mileage may vary.
#
# Copyright (C)2009 Robert Varga <varga@pantheon.sk>
# Licensed under GNU General Public License version 2, available at http://www.gnu.org/licenses/old-licenses/gpl-2.0.html
#
# DISCLAIMER: By using this program you acknowledge that it is completely
#             unsupported and you accept responsibility for any and all
#             consequences stemming from your usage. I may not be held
#             accountable for any corruption and/or data loss this software causes.

# Your mailstore prefix. This script handles only one mail store -- sorry :)
store_prefix = "/data/zimbra"
# Use 'zmlocalconfig -s zimbra_mysql_password' to find out what your password is
mysql_password = ''
# Read concurrency tunable
reader_count = 16
# Transform concurrency tunable: set to your CPU count
transformer_count = 2
# Write concurrency tunable
writer_count = 16

# These are semi-tunable
temp_suffix = '.crlf'
mysql_host = '127.0.0.1'
mysql_port = 7306
mysql_user = 'zimbra'

from hashlib import sha1
from base64 import b64encode
from os import path
from shutil import move
from threading import Thread
from Queue import Queue
from MySQLdb import connect

class FileHandle:
	def __init__(self, mid, fid, fmod, size, digest):
		self.path = store_prefix + '/' + str(mid >> 12) + '/' + str(mid) + '/msg/' + str(fid >> 12) + '/' + str(fid) + '-' + str(fmod) + '.msg'
		self.mid = mid
		self.fid = fid
		self.fmod = fmod
		self.size = size
		self.digest = digest

class FileToValidateDigest:
	def __init__(self, handle, data):
		self.handle = handle
		self.data = data

class FileToWrite:
	def __init__(self,handle , data, digest):
		self.handle = handle
		self.data = data
		self.digest = digest

class FileToUpdate:
	def __init__(self, handle, size, digest):
		self.handle = handle
		self.size = size
		self.digest = digest

def readFile():
	while True:
		h = readq.get();
		if path.exists(h.path):
			if path.isfile(h.path):
				size = path.getsize(h.path)
				if size == h.size:
					infile = open(h.path, "r")
					data = infile.read()
					transformq.put(FileToValidateDigest(h, data), True)
					infile.close()
				else:
					print "Skipping file", h.path, "due to size mismatch:", h.size, "expected,", size, "actual"
			else:
				print "Skipping non-regular file", h.path
		else:
			print "Skipping non-existent file", h.path
		readq.task_done()

def transformFile():
	while True:
		f = transformq.get();
		digest = b64encode(sha1(f.data).digest()).replace('/', ',')

		if (f.handle.digest == digest):
			data = f.data.replace('\n', '\r\n').replace('\r\r\n', '\r\n')
			if (len(data) != f.handle.size):
				nd = b64encode(sha1(data).digest()).replace('/', ',')
				writeq.put(FileToWrite(f.handle, data, nd), True)
			else:
				print "Skipping unchanged file", f.handle.path
		else:
			print "Skipping file", f.handle.path, "due to digest mismatch:", f.handle.digest, "expected,", digest, " actual"
		transformq.task_done();

def writeFile():
	while True:
		f = writeq.get()
		h = f.handle
		o = open(h.path + temp_suffix, 'w')
		o.write(f.data)
		o.close()
		updateq.put(FileToUpdate(h, len(f.data), f.digest), True)
		writeq.task_done()

print "Creating queues"
readq = Queue()
transformq = Queue(15)
writeq = Queue()
updateq = Queue()

print "Creating", reader_count, "read workers"
readers = [ ]
for i in range(reader_count):
	t = Thread(target=readFile, name='Read-'+str(i))
	t.daemon = True
	t.start()
	readers.append(t)

print "Creating", transformer_count, "transform workers"
transformers = [ ]
for i in range(transformer_count):
	t = Thread(target=transformFile, name='Transform'+str(i))
	t.daemon = True
	t.start()
	transformers.append(t)

print "Creating", writer_count, "write workers"
writers = [ ]
for i in range(writer_count):
	t = Thread(target=writeFile, name='Write-'+str(i))
	t.daemon = True
	t.start()
	writers.append(t)

print "Fetching users"
conn = connect(host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_password, db='zimbra');
cursor = conn.cursor()
cursor.execute('SELECT id,comment FROM mailbox ORDER BY comment')
users = cursor.fetchall()
cursor.close()
conn.close()

for u in users:
	id = u[0]
	name = u[1]
	store = id % 100
	if (store == 0):
		store = 100

	conn = connect(host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_password, db='mboxgroup' + str(store))
	cursor = conn.cursor()
	cursor.execute('SELECT mailbox_id, id, mod_content, size, blob_digest FROM mail_item WHERE mailbox_id = %s AND volume_id IS NOT NULL', (id))
	count = 0

	for row in cursor:
		readq.put(FileHandle(row[0], row[1], row[2], row[3], row[4]))
		count += 1
	cursor.close()

	print "Processing user", id, name, "store", store, "mail count:", count
	readq.join()
	print "Read done"
	transformq.join()
	print "Transform done"
	writeq.join()
	print "Write done"

	cursor = conn.cursor()
	count = 0
	while not updateq.empty():
		f = updateq.get()
		h = f.handle
		print "Will update file", h.path, "digest", h.digest, "->", f.digest, "size", h.size, "->", f.size
		cursor.execute('UPDATE mail_item SET size = %s, blob_digest = %s WHERE mailbox_id = %s AND id = %s', (f.size, f.digest, h.mid, h.fid))
		move(h.path + temp_suffix, h.path)

		updateq.task_done()
		count += 1
	cursor.close()

	print "Done updating", count, "files"
