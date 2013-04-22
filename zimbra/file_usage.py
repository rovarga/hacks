#!/usr/bin/env python
# vi: tabstop=4 smarttab shiftwidth=4
#
# Copyright (C)2013 Robert Varga <varga@pantheon.sk>
# Licensed under GNU General Public License version 2, available at
# http://www.gnu.org/licenses/old-licenses/gpl-2.0.html
#
# DISCLAIMER: By using this program you acknowledge that it is completely
#             unsupported and you accept responsibility for any and all
#             consequences stemming from your usage. I may not be held
#             accountable for any corruption and/or data loss this software
#             causes.

# Input file is in the format:
# <inodeNumber> <size> <blockCount> <linkCount> <fileName>

import collections
import fileinput
import string

FileEntry = collections.namedtuple('FileEntry', ['inode', 'size', 'blocks', 'links', 'paths'])
files = collections.OrderedDict()

for line in fileinput.input():
	(inode, size, blocks, links, path) = string.split(line.rstrip('\n'), ' ')
	if not inode in files:
		files[inode] = FileEntry(long(inode), long(size), long(blocks), long(links), set())
	files[inode].paths.add(path)
	
blocksUsed = 0
bytesUsed = 0
blocksSaved = 0
bytesSaved = 0

for entry in files.itervalues():
	blocksUsed += entry.blocks
	bytesUsed += entry.size
	if entry.links > 1:
		blocksSaved += (entry.links - 1) * entry.blocks
		bytesSaved += (entry.links - 1) * entry.size

print str(blocksUsed) + " blocks, " + str(bytesUsed) + " bytes used"
print str(bytesUsed / len(files)) + " average file size"
print str(((blocksUsed * 512) - bytesUsed) / len(files)) + " bytes wasted per file"
print str(blocksSaved) + " blocks, " + str(bytesSaved) + " bytes saved on sharing"

#sizes = dict()
#	if not size in sizes:
#		sizes[size] = set()
#	sizes[size].add(inode)

#for inodes in sizes.itervalues():
#	if len(inodes) > 1:
#		print "Candidate files found:"
#		for inode in inodes:
#			entry = files[inode]
#			print "Inode " + str(entry.inode) + " size " + str(entry.size) + " files " + str(entry.paths)

