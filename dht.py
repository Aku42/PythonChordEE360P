from chord import Local, Daemon, repeat_and_sleep, inrange
from remote import Remote
from address import Address
import json
import socket
import time
import traceback
import thread
# data structure that represents a distributed hash table
class DHT(object):
	def __init__(self, local_address, remote_address = None):
		self.local_ = Local(local_address, remote_address)
		def set_wrap(msg):
			return self._set(msg)
		def get_wrap(msg):
			return self._get(msg)
		def backup_wrap(msg):
			return self._backup(msg)
		def dump_wrap(msg):
			return self.dump()
		def dumpall_wrap(msg):
			return self.dumpall()

		self.data_ = {}
		self.backup_ = {}
		self.shutdown_ = False

		self.local_.register_command("set", set_wrap)
		self.local_.register_command("get", get_wrap)
		self.local_.register_command("backup", backup_wrap)
		self.local_.register_command("dump", dump_wrap)
		self.local_.register_command("dumpall", dumpall_wrap)

		self.daemons_ = {}
		self.daemons_['distribute_data'] = Daemon(self, 'distribute_data')
		self.daemons_['distribute_data'].start()

		self.local_.start()

	def shutdown(self):
		self.local_.shutdown()
		self.shutdown_ = True

	def _get(self, request):
		try:
			data = json.loads(request)
			# we have the key
			return json.dumps({'status':'ok', 'data':self.get(data['key'])})
		except Exception:
			# key not present
			traceback.print_exc()
			return json.dumps({'status':'failed'})

	def _set(self, request):
		try:
			data = json.loads(request)
			key = data['key']
			value = data['value']
			self.set(key, value)
			return json.dumps({'status':'ok'})
			# something is not working
		except Exception:
			traceback.print_exc()
			return json.dumps({'status':'failed'})

	def get(self, key):
		try:
			return self.data_[key]['value']
		except Exception:
			# not in our range
			suc = self.local_.find_successor(hash(key))
			if self.local_.id() == suc.id():
				# it's us but we don't have it
				return None
			try:
				response = suc.command('get %s' % json.dumps({'key':key}))
				if not response:
					raise Exception
				value = json.loads(response)
				if value['status'] != 'ok':
					raise Exception
				return value['data']
			except Exception:
				return None

	def sendbackup(self):
		temp = self.local_.successor()
		for i in [0,1]:
		#while temp != self.local_:
			print "Trying backup at %s" % temp.id()
			response = temp.command('backup %s' % json.dumps({"id":self.local_.id(),"data":self.data_}))
			temp = temp.successor()
		print "Done backing up"

	def setactual(self, suc, key, value):
		suc.command('set %s' % json.dumps({'key':key,'value':value}))

	def set(self, key, value):
		# get successor for key
		self.data_[key]={"value":value, 'backedUp':False}
		suc = self.local_.find_successor(hash(key))
		if self.local_.id() == suc.id():
			#its us
			self.data_[key] = value
			print "Stored at %s" % self.local_.id()
			try:
				self.sendbackup()
				return True
			except Exception:
				print traceback.print_exc(Exception)
		return True

	def _backup(self, request):
		print "Backing up: " + request + " at %s" % self.local_.id()
		try:
			data = json.loads(request)
			id = data['id']
			value = data['data']
			self.backup(id, value)
			return json.dumps({'status':'ok'})
			# something is not working
		except Exception:
			traceback.print_exc()
			return json.dumps({'status':'failed'})

	def backup(self, id, value):
		self.backup_[id]=value
		return True

	def dump(self):
		dumpStr = "ID: %s" % self.local_.id() + "\n"\
				+ "Data: %s" % json.dumps(self.data_) + "\n"\
				+ "Backup: %s" % json.dumps(self.backup_) + "\n"
		return dumpStr

	def dumpall(self):
		retStr = self.dump() + "\n\n"
		temp = self.local_.successor()
		temp2 = None
		i = 0
		while 1:
			temp2 = self.local_
			retStr+=temp.command("dump")+"\n\n"
			temp = temp.successor()
			#print str(temp.id()) + ":" +str(temp2.id())
			if temp.id()==temp2.id():
				break
		return retStr


	@repeat_and_sleep(5)
	def distribute_data(self):
		to_remove = []
		# to prevent from RTE in case data gets updated by other thread
		keys = self.data_.keys()
		for key in keys:
			if self.local_.predecessor() and \
			   not inrange(hash(key), self.local_.predecessor().id(1), self.local_.id(1)):
				try:
					node = self.local_.find_successor(hash(key))
					node.command("set %s" % json.dumps({'key':key, 'value':self.data_[key]}))
					# print "moved %s into %s" % (key, node.id())
					to_remove.append(key)
					print "migrated"
				except socket.error:
					print "error migrating"
					# we'll migrate it next time
					pass
		# remove all the keys we do not own any more
		for key in to_remove:
			del self.data_[key]
		# Keep calling us
		return True

def create_dht(lport):
	#laddress = map(lambda port: Address('127.0.0.1', port), lport)
	r = [DHT(lport[0])]
	for address in lport[1:]:
		r.append(DHT(address, lport[0]))
		time.sleep(.5)
	return r


if __name__ == "__main__":
	import sys
	if len(sys.argv) == 2:
		dht = DHT(Address("127.0.0.1", sys.argv[1]))
	else:
		dht = DHT(Address("127.0.0.1", sys.argv[1]), Address("127.0.0.1", sys.argv[2]))
	raw_input("Press any key to shutdown")
	print "shuting down.."
	dht.shutdown()

