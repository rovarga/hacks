#
# Simple sed script to process the output of 'find /path -print0 | xargs --null stat'
# on Linux machines. This will concatenate the output and create a single line
# containing in the format:
#
# <inodeNumber> <size> <blockCount> <linkCount> <fileName>
#
# Which is suitable for processing using the 'file_stat.py' script.
#
# Copyright (C)2013 Robert Varga <varga@pantheon.sk>
# Licensed under GNU General Public License version 2, available at
# http://www.gnu.org/licenses/old-licenses/gpl-2.0.html
N
N
N
N
N
N
s/^  File: `\(.*\)'.*Size: \([0-9]\+\).*Blocks: \([0-9]\+\).*Inode: \([0-9]\+\).*Links: \([0-9]\+\).*/\4 \2 \3 \5 \1/
p
