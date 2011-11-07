#!/usr/bin/python
# -*- coding: utf-8 -*-

#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU Lesser General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU Lesser General Public License for more details.
#
#   You should have received a copy of the GNU Lesser General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>

from sstable.sstable import sstable
import struct, string, random, time

# Create a string with 5 keys inside
# payload size = 10
buf = struct.pack("8sHHIIIIIII15s15s15s15s15s", "YPySSTbl", 1, 10, 5, 0, 15, 30, 45, 60, 75, "AAkey0123456789", "FFKeyFFFFFFFFFf", "MMKey9876543210", "QQKeyQQQQQQQQQq", "zzKeyABCDEFGHIJ")
s = sstable(buf)
s.load()

# Search for a key
print s.search("zzKey")

# Overwrite key
s.insert("zzKey", "0011223344", True)
print s.search("zzKey")



# Reinitialize sstable, set payload size = 10
s.init(10)
# Insert some keys
s.insert("zzKey", "ABCDEFGHIJ")
print s.search("zzKey")
s.insert("QQKey", "QQQQQQQQQq")

# Do benchmarking
print "Filling SSTable with 1000 random records:"
start = time.time()
for count in xrange(1,1000):
	key = "".join(random.sample(string.letters+string.digits, random.randint(5,50)))
	s.insert(key, "1122334455")
print "    Done in " + str(time.time() - start) + " seconds"

print "Adding 10 more random records:"
start = time.time()
for count in xrange(1,10):
	key = "".join(random.sample(string.letters+string.digits, random.randint(5,50)))
	s.insert(key, "1122334455")
print "    Done in " + str(time.time() - start) + " seconds"

# Check keys inserted at the beginning
print s.search("zzKey")
print s.search("QQKey")

# Delete it and check again
s.delete("QQKey")
print s.search("zzKey")
try:
	print s.search("QQKey")
except KeyError as e:
	print e

try:
	s.delete("QQKey")
except KeyError as e:
	print e
