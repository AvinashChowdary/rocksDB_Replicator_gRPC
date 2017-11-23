'''
################################## replicator.py #############################
# replicator to manage data in one more db other than server
################################## replicator.py #############################
'''
import grpc
import rocksdb
import argparse
import datastore_pb2

PORT = 3000

class Replicator():
	
	def __init__(self, host='0.0.0.0', port=PORT):
		self.db = self.db = rocksdb.DB("replicator_db.db", rocksdb.Options(create_if_missing=True))
		self.channel = grpc.insecure_channel('%s:%d' % (host, port))
		self.stub = datastore_pb2.ReplicatorStub(self.channel)

	def replicate(self):
		tasks = self.stub.alive_connection(datastore_pb2.ReplicateRequest())

		for task in tasks:
			if task.type == 'save_to_db':
				print("put in replicator")
				key = (task.key).encode(encoding='UTF-8',errors='strict')
				data = (task.data).encode(encoding='UTF-8',errors='strict')
				self.db.put(key, data)
			
			elif task.type == 'delete_from_db':
				print("delete in replicator")
				key = (task.key).encode(encoding='UTF-8',errors='strict')
				self.db.delete(key)

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("host", help="display a square of a given number")
	args = parser.parse_args()
	print("Replicator is connecting to Server at {}:{}...".format(args.host, PORT))
	replicator = Replicator(host=args.host)
	replicator.replicate()
				
if __name__ == "__main__":
	main()
	