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

import types
import struct, string, random, time

class HeaderException(Exception):
	pass

class sstable:

	# Internal data buffer
	buffer = ""

	# Header format:
	# 8 bytes: magic
	# 2 bytes: version
	# 2 bytes: chunk size
	# 4 bytes: number of chunks inside
	header_fmt = "<8sHHI"

	# Chunk header format:
	# 2 bytes: length in chunks
	# 2 bytes: current chunk
	# 2 bytes: key size
	# 2 bytes: payload size
	chunk_header_fmt = "<HHHH"
	chunk_header_size = 8

	header_magic = "YPySSTbl"
	header_version = 2
	header_chunk_size = 0
	header_count = 0
	header_strings_start = 0

	def __init__(self, buffer=None):
		self.header_count = 0
		self.header_chunk_size = 0
		self.header_strings_start = 0
		self.buffer = ""

		if type(buffer) == types.StringType:
			self.buffer = buffer

	def init(self, chunk_size):
		# Put header into buffer
		self.buffer = struct.pack(self.header_fmt, self.header_magic, self.header_version, chunk_size, 0)

		self.header_count = 0
		self.header_chunk_size = chunk_size
		self.header_strings_start = struct.calcsize(self.header_fmt)

	def load(self, buffer=None):
		if type(buffer) == types.StringType:
			self.buffer = buffer

		header = struct.unpack_from(self.header_fmt, self.buffer, 0)
		self.header_strings_start = struct.calcsize(self.header_fmt)

		# Compare magic and version
		if header[0] != self.header_magic:
			raise HeaderException('Incorrect header magic')

		if header[1] != self.header_version:
			raise HeaderException('Incorrect header version')

		self.header_chunk_size = header[2]
		self.header_count = header[3]

	def get_rec_by_offset(self, offset):
		chunk_header = struct.unpack_from(self.chunk_header_fmt, self.buffer, offset)
		if chunk_header[1] > 0:
			offset -= chunk_header[1]*self.header_chunk_size

		chunk_header = struct.unpack_from(self.chunk_header_fmt, self.buffer, offset)

		key_len = chunk_header[2]
		payload_len = chunk_header[3]

		key = ""
		payload = ""
		rec_offset = 0

		for i in xrange(0, chunk_header[0]):
			key_size = 0

			# Get key from chunks
			if key_len > 0:
				key_size = self.header_chunk_size - self.chunk_header_size
				if key_len < key_size:
					key_size = key_len

				key_offset = offset + rec_offset + self.chunk_header_size
				key += self.buffer[key_offset:(key_offset + key_size)]
				key_len -= key_size

			# Get payload from chunks
			if key_len == 0 and payload_len > 0:
				payload_size = self.header_chunk_size - self.chunk_header_size - key_size
				if payload_len < payload_size:
					payload_size = payload_len

				payload_offset = offset + rec_offset + self.chunk_header_size + key_size
				payload += self.buffer[payload_offset:(payload_offset + payload_size)]
				payload_len -= payload_size

			rec_offset += self.header_chunk_size
		return (key, payload, offset, chunk_header[0])

	def chunks_num_by_size(self, size):
		ins_chunks = float(size)/(self.header_chunk_size - self.chunk_header_size)
		if ins_chunks != int(ins_chunks):
			ins_chunks = int(ins_chunks) + 1
		else:
			ins_chunks = int(ins_chunks)
		return ins_chunks

	def make_chunks_from_rec(self, key, payload):
		chunk_num = self.chunks_num_by_size(len(key) + len(payload))

		key_len = len(key)
		payload_len = len(payload)
		max_chunk_len = self.header_chunk_size - self.chunk_header_size

		chunks = ""
		key_offset = 0
		payload_offset = 0

		for i in xrange(0, chunk_num):
			local_offset = 0
			chunks += struct.pack(self.chunk_header_fmt, chunk_num, i, len(key), len(payload))

			if key_len > 0:
				size = key_len
				if size > max_chunk_len:
					size = max_chunk_len

				chunks += key[key_offset:key_offset+size]
				key_len -= size
				key_offset += size
				local_offset = size

			if key_len == 0 and payload_len > 0:
				size = payload_len
				if size > (max_chunk_len - local_offset):
					size = max_chunk_len - local_offset

				chunks += payload[payload_offset:payload_offset+size]
				payload_len -= size
				payload_offset += size
				local_offset = size

		return (chunks.ljust(chunk_num * self.header_chunk_size), chunk_num)

	def search(self, key, equal=True):
		if len(key) == 0:
			raise KeyError("Key should not be empty")

		min = 0
		max = self.header_count-1

		while min <= max:
			mid = int((max + min)/2)

			offset = self.header_strings_start + mid * self.header_chunk_size
			rec = self.get_rec_by_offset(offset)

			if key == rec[0]:
				return rec

			if key < rec[0]:
				max = mid-1
			else:
				min = mid+1
		if not equal:
			return rec

	def insert(self, key, payload, overwrite=False):
		if len(key) == 0:
			raise KeyError("Key should not be empty")

		rem_count = 0
		# If structure is empty
		if self.header_count == 0:
			offset = self.header_strings_start
			offset_tail = offset
		else:
			rec = self.search(key, False)
			offset = rec[2]

			if rec[0] < key:
				offset += rec[3] * self.header_chunk_size

			offset_tail = offset

			if rec[0] == key:
				if not overwrite:
					raise KeyError("Duplicate key")
				offset_tail += rec[3] * self.header_chunk_size
				rem_count = rec[3]

		# Pack data into chunks
		chunks = self.make_chunks_from_rec(key, payload)

		# Create header
		new_buf = struct.pack(self.header_fmt, self.header_magic, self.header_version, self.header_chunk_size, self.header_count+chunks[1] - rem_count)

		# Create main body
		new_buf += self.buffer[self.header_strings_start:offset] \
			+ chunks[0] \
			+ self.buffer[offset_tail:(self.header_strings_start + self.header_count * self.header_chunk_size)]

		self.buffer = new_buf
		self.header_count += chunks[1] - rem_count

	def delete(self, key):
		if len(key) == 0:
			raise KeyError("Key should not be empty")

		rec = self.search(key)
		if not rec:
			raise KeyError("key doesn't exist")

		offset = rec[2]
		offset_tail = offset + rec[3] * self.header_chunk_size

		# Create header
		new_buf = struct.pack(self.header_fmt, self.header_magic, self.header_version, self.header_chunk_size, self.header_count-rec[3])

		# Create main body
		new_buf += self.buffer[self.header_strings_start:offset] \
			+ self.buffer[offset_tail:(self.header_strings_start + self.header_count * self.header_chunk_size)]

		self.buffer = new_buf
		self.header_count -= rec[3]

	def save(self):
		return self.buffer

