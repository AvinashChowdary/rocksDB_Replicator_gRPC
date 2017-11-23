'''
################################## server.py #############################
#gRPC RocksDB Server to save data and replicate in replicator
################################## server.py #############################
'''
import time
import grpc
import uuid
import queue
import rocksdb
import datastore_pb2
import datastore_pb2_grpc
from concurrent import futures


_ONE_DAY_IN_SECONDS = 60 * 60 * 24

class MyDatastoreServicer(datastore_pb2.ReplicatorServicer):
	def __init__(self):
		self.db = rocksdb.DB("server_db.db", rocksdb.Options(create_if_missing=True))
		self.tasks = queue.Queue()

	def get(self, request, context):
		print("get")
		# retrieving the value from DB by the given key 
		key = (request.data).encode(encoding='UTF-8',errors='strict')
		value = self.db.get(key)
		return datastore_pb2.Response(data=value)

	def alive_connection(self, request, context):
		print("Attemting replicator connection")
		while True:
			while not self.tasks.empty():
				yield self.tasks.get()

	def decorate(type):
		def wrapper(self, key, request, context):
			# converting key and value string to utf-8 bytes 
			bkey = key.encode(encoding='UTF-8',errors='strict')
			value = (request.data).encode(encoding='UTF-8',errors='strict')

			call = datastore_pb2.ReplicateResponse(key = bkey, data = value, type = type.__name__)
			self.tasks.put(call)
			return type(self, key, request, context)
		return wrapper
  
	@decorate
	def save_to_db(self, key, request, context):
		# converting key and value string to utf-8 bytes 
		bkey = key.encode(encoding='UTF-8',errors='strict')
		value = (request.data).encode(encoding='UTF-8',errors='strict')
		# storing key and value as bytes in rocksDB
		self.db.put(bkey, value)
		return datastore_pb2.Response(data=key)

	def put(self, request, context):
		print("put")
		key = uuid.uuid4().hex
		return self.save_to_db(key, request, context)

	@decorate
	def delete_from_db(self, key, request, context):
		# converting key to utf-8 bytes 
		bkey = (request.data).encode(encoding='UTF-8',errors='strict')
		self.db.delete(bkey)
		deleted = "deleted"
		response = deleted.encode(encoding='UTF-8',errors='strict')
		return datastore_pb2.Response(data=response)

	def delete(self, request, context):
		print("delete")
		key = request.data
		return self.delete_from_db(key, request, context)

def run(host, port):
	'''
	Run the GRPC server
	'''
	server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
	datastore_pb2_grpc.add_ReplicatorServicer_to_server(MyDatastoreServicer(), server)
	server.add_insecure_port('%s:%d' % (host, port))
	server.start()

	try:
		while True:
			print("Server started at...%d" % port)
			time.sleep(_ONE_DAY_IN_SECONDS)
	except KeyboardInterrupt:
		server.stop(0)


if __name__ == '__main__':
	run('0.0.0.0', 3000)