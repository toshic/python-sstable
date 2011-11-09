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

import struct, array, types


# Structure of SSTable file:
# * 16 bytes of header (see below in sstable class)
# * Array of offsets. Each offset points to start of the string. For the first string offset = 0.
# * One extra offset equals points to the byte right after last string+payload
# * Strings. Each string is followed by payload, to calculate size of string 
#   calculate difference between current and next offsets and decrease by size of payload
#
# All numbers inside are unsigned, all counters and offsets are integers



class HeaderException(Exception):
	pass

class sstable:

	# Internal data buffer
	buffer = ""

	# Header format:
	# 8 bytes: magic
	# 2 bytes: version
	# 2 bytes: size of extra structure
	# 4 bytes: number of strings inside
	header_fmt = "8sHHI"
	header_magic = "YPySSTbl"
	header_version = 1
	header_payload_size = 0
	header_count = 0
	header_strings_start = 0

	# Indexes
	# Just plain array of unsigned integers
	index = array.array("I")

	def __init__(self, buffer=None):
		self.index = array.array("I")
		self.header_count = 0
		self.header_payload_size = 0
		self.header_strings_start = 0
		self.buffer = ""

		if type(buffer) == types.StringType:
			self.buffer = buffer

	def init(self, payload_size):
		# Put header and one extra index indicates size=0 into buffer
		self.buffer = struct.pack(self.header_fmt + "I", self.header_magic, self.header_version, payload_size, 0, 0)

		self.index = array.array("I")
		self.index.insert(0, 0)
		self.header_count = 0
		self.header_payload_size = payload_size
		self.header_strings_start = struct.calcsize(self.header_fmt) + self.index.itemsize

	def load(self, buffer=None):
		if type(buffer) == types.StringType:
			self.buffer = buffer

		position = 0
		header = struct.unpack_from(self.header_fmt, self.buffer, position)
		position += struct.calcsize(self.header_fmt)

		# Compare magic and version
		if header[0] != self.header_magic:
			raise HeaderException('Incorrect header magic')

		if header[1] != self.header_version:
			raise HeaderException('Incorrect header version')

		self.header_payload_size = header[2]
		self.header_count = header[3]

		# Load indexes
		# There is one extra index at the end that stores total length
		self.index.fromstring(self.buffer[position:(position + (self.header_count+1) * self.index.itemsize)])

		self.header_strings_start = position + (self.header_count+1) * self.index.itemsize

	def search(self, key):
		# Use binary search 'cause strings are sorted
		# There is one extra index at the end that stores total length
		min = 0
		max = self.header_count-1

		while min <= max:
			mid = int((min + max) / 2)

			# Get offset and calculate length of the record
			length = self.index[mid+1] - self.index[mid]
			offset = self.index[mid]

			# Each record consists of key and fixed-size payload
			record = self.buffer[(self.header_strings_start + offset):(self.header_strings_start + offset + length)]
			string = record[:length-self.header_payload_size]
			payload = record[length-self.header_payload_size:]

			if string == key:
				return (mid, key, payload)

			if key > string:
				min = mid+1
			else:
				max = mid-1
		raise KeyError("Key doesn't exist")

	def insert(self, key, payload, rewrite=False):

		if len(payload) != self.header_payload_size:
			raise KeyError("Incorrect payload size")

		# Use binary search 'cause strings are sorted
		# There is one extra index at the end that stores total length
		mid = 0
		min = 0
		max = self.header_count-1
		string = ""

		while min <= max:
			mid = int((min + max) / 2)

			length = self.index[mid+1] - self.index[mid]
			offset = self.index[mid]

			# Each record consists of key and fixed-size payload
			record = self.buffer[(self.header_strings_start + offset):(self.header_strings_start + offset + length)]
			string = record[:length-self.header_payload_size]

			if string == key:
				break

			if key > string:
				min = mid+1
			else:
				max = mid-1

		idx = mid
		if string == key:
			if not rewrite:
				raise KeyError("Duplicate key")
			new_buf = self.buffer[:(self.header_strings_start + offset + length - self.header_payload_size)] \
				+ payload + self.buffer[(self.header_strings_start + offset + length):]
			self.buffer = new_buf
		else:
			if key > string:
				idx += 1

			# If it is a first record
			if self.header_count == 0:
				idx = 0

			# New record should be added at position idx
			# all offsets from this position to the end should be increased with record length
			offset = self.index[idx]
			length = len(key) + self.header_payload_size

			self.index.insert(idx, offset)
			self.header_count += 1
			for i in xrange(idx+1, self.header_count+1):
				self.index[i] += length

			# Create header
			new_buf = struct.pack(self.header_fmt, self.header_magic, self.header_version, self.header_payload_size, self.header_count)
			# Create new buffer
			new_buf += self.index.tostring() + self.buffer[self.header_strings_start:(self.header_strings_start + offset)] \
				+ key + payload + self.buffer[(self.header_strings_start + offset):]
			self.buffer = new_buf
			self.header_strings_start += self.index.itemsize

	def delete(self, key):
		res = self.search(key)
		if not res:
			raise KeyError("key doesn't exist")

		idx = res[0]
		offset = self.index[idx]
		length = len(key) + self.header_payload_size

		# Remove idx from array
		# all offsets from this position to the end should be decreased with record length
		self.index.pop(idx)
		self.header_count -= 1
		for i in xrange(idx, self.header_count+1):
			self.index[i] -= length

		# Create header
		new_buf = struct.pack(self.header_fmt, self.header_magic, self.header_version, self.header_payload_size, self.header_count)

		# Create new buffer
		new_buf += self.index.tostring() + self.buffer[self.header_strings_start:(self.header_strings_start + offset)] \
			+ self.buffer[(self.header_strings_start + offset + length):]
		self.buffer = new_buf
		self.header_strings_start -= self.index.itemsize

	def save(self):
		return self.buffer

