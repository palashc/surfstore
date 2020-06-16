import os
import json
import hashlib
import argparse
import xmlrpc.client
import base64

INDEX_FILE = "index.txt"
NEW_FILE_VERSION = 1
TOMBSTONE_HASHLIST = ["0"]

def get_full_file_path(file):
	return os.path.join(BASE_DIR, file)

# Read file in chunks defined by BLOCK_SIZE
def get_file_blocks(fp):
	while True:
		data = fp.read(BLOCK_SIZE)
		if not data:
			break
		yield data

# Hash the Block List and return Hash List
def get_hash_list(file):
	hashlist = []
	filename = get_full_file_path(file)
	fp = open(filename, "rb")
	for block in get_file_blocks(fp):
		hash_value = hashlib.sha256(block).hexdigest()
		hashlist.append(hash_value)

	fp.close()
	return hashlist

# Scan BASE_DIR and get file -> hashlist map
def scan_base_dir():
	index = {}
	#scan only files, skip dirs and subdirs and index.txt
	file_list = [f for f in os.listdir(BASE_DIR) if f != INDEX_FILE and os.path.isfile(os.path.join(BASE_DIR, f))]
	for file in file_list:
		hashlist = get_hash_list(file)
		if not hashlist:
			continue
		index[file] = [NEW_FILE_VERSION, hashlist]

	return index

# Create index.txt if not present. Read index.txt and build local index.
def build_local_index():
	local_index = {}
	index_file_name = get_full_file_path(INDEX_FILE)
	if not os.path.exists(index_file_name):
		open(index_file_name, "w+")

	fp = open(index_file_name, "r")
	for file_info in fp.readlines():
		file_info_parts = file_info.split()
		local_index[file_info_parts[0]] = [int(file_info_parts[1]), file_info_parts[2:]]

	fp.close()
	return local_index

# Save updated local index to index.txt
def save_local_index(local_index):
	with open(get_full_file_path(INDEX_FILE), "w") as fp:
		for key in local_index.keys():
			fp.write(" ".join([key, str(local_index[key][0]), " ".join(local_index[key][1])]))
			fp.write("\n")

# Update local index for given file with given file info
def update_local_indices(file, fileinfo):
	global local_index
	global base_dir_index
	local_index[file] = fileinfo
	base_dir_index[file] = fileinfo
	print(file, local_index, base_dir_index)

# Download the given file blocks from the server
def download_file_blocks(file, fileinfo):
	version = fileinfo[0]
	hashlist = fileinfo[1]

	#check if tombstone
	if hashlist == TOMBSTONE_HASHLIST:
		filename = get_full_file_path(file)
		if os.path.exists(filename):
			os.remove(filename)
	else:
		fp = open(get_full_file_path(file), "wb");
		for _hash in hashlist:
			block = rpc_client.surfstore.getblock(_hash)
			fp.write(block)
		fp.close()

# Upload file blocks to the server
def upload_file_blocks(file):
	filename = get_full_file_path(file)
	fp = open(filename, "rb")
	for block in get_file_blocks(fp):
		rpc_client.surfstore.putblock(block)

def download_file_and_update_local_indices(file, fileinfo):
	download_file_blocks(file, fileinfo)
	update_local_indices(file, fileinfo)


# SYNC
def sync(base_dir_index, local_index, remote_index):

	# download new file
	for file in remote_index.keys():
		print("Downloading:", file)
		if file not in local_index and file not in base_dir_index:
			download_file_and_update_local_indices(file, remote_index[file])
	print("Done checking remote new")
	for file in base_dir_index.keys():
		#upload new file
		print("Checking for local new")
		if file not in remote_index:
			print("Found local new:", file)
			upload_file_blocks(file)
			status = rpc_client.surfstore.updatefile(file, base_dir_index[file][0], base_dir_index[file][1])
			if status: #upload was successful on server
				update_local_indices(file, base_dir_index[file])
			else: #upload failed
				new_remote_index = rpc_client.surfstore.getfileinfomap()
				download_file_and_update_local_indices(file, new_remote_index[file])

		elif file in local_index and file in remote_index:
			# no local change, only remote change
			if base_dir_index[file][1] == local_index[file][1]:
				if remote_index[file][0] > local_index[file][0]:
					download_file_and_update_local_indices(file, remote_index[file])
			else: #there is local change
				#local and remote have same version
				if remote_index[file][0] == local_index[file][0]:
					upload_file_blocks(file)
					status = rpc_client.surfstore.updatefile(file, remote_index[file][0]+1, base_dir_index[file][1])
					if status:
						update_local_indices(file, [remote_index[file][0]+1, base_dir_index[file][1]])
					else:
						new_remote_index = rpc_client.surfstore.getfileinfomap()
						download_file_and_update_local_indices(file, new_remote_index[file])

				elif remote_index[file][0] > local_index[file][0]:
					download_file_and_update_local_indices(file, remote_index[file])

		elif file not in local_index and file in remote_index:
			upload_file_blocks(file)
			status = rpc_client.surfstore.updatefile(file, base_dir_index[file][0], base_dir_index[file][1])
			if status: #upload was successful on server
				update_local_indices(file, base_dir_index[file])
			else: #upload failed
				new_remote_index = rpc_client.surfstore.getfileinfomap()
				download_file_and_update_local_indices(file, new_remote_index[file])


	#deletes from local
	for file in local_index:
		if file not in base_dir_index:
			status = rpc_client.surfstore.updatefile(file, local_index[file][0]+1, TOMBSTONE_HASHLIST)
			if status:
				update_local_indices(file, [local_index[file][0]+1, TOMBSTONE_HASHLIST])
			else:
				new_remote_index = rpc_client.surfstore.getfileinfomap()
				download_file_and_update_local_indices(file, new_remote_index[file])

	save_local_index(local_index)


if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="SurfStore client")
	parser.add_argument('hostport', help='host:port of the server')
	parser.add_argument('basedir', help='The base directory')
	parser.add_argument('blocksize', type=int, help='Block size')
	args = parser.parse_args()

	try:
		rpc_client  = xmlrpc.client.ServerProxy(uri="http://"+args.hostport, use_builtin_types=True)
		BASE_DIR = os.path.abspath(os.path.join(args.basedir, ''))
		BLOCK_SIZE = args.blocksize
		base_dir_index = scan_base_dir()
		local_index = build_local_index()
		remote_index = rpc_client.surfstore.getfileinfomap()
		sync(base_dir_index, local_index, remote_index)

	except Exception as e:
		print("Client: " + str(e))
