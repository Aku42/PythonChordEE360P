from chord import Local, Daemon, repeat_and_sleep, retry_on_socket_error, inrange
from remote import Remote
from address import Address
import json
import socket
import time
import traceback
from settings import *
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

		self.backupFlag = False

		self.local_.register_command("set", set_wrap)
		self.local_.register_command("get", get_wrap)
		self.local_.register_command("backup", backup_wrap)
		self.local_.register_command("dump", dump_wrap)
		self.local_.register_command("dumpall", dumpall_wrap)
		self.local_.stabilize = self.stabilize
		self.local_.update_successors = self.update_successors
		self.daemons_ = {}
		self.daemons_['distribute_data'] = Daemon(self, 'distribute_data')
		self.daemons_['distribute_data'].start()
		self.daemons_['sendbackup'] = Daemon(self, 'sendbackup')
		self.daemons_['sendbackup'].start()

		self.local_.start()

	@repeat_and_sleep(STABILIZE_INT)
	@retry_on_socket_error(STABILIZE_RET)
	def stabilize(self):
		self.local_.log("stabilize")
		suc = self.local_.successor()
		# We may have found that x is our new successor iff
		# - x = pred(suc(n))
		# - x exists
		# - x is in range (n, suc(n))
		# - [n+1, suc(n)) is non-empty
		# fix finger_[0] if successor failed
		if suc.id() != self.local_.finger_[0].id():
			self.local_.finger_[0] = suc
		x = suc.predecessor()
		if x != None and \
		   inrange(x.id(), self.local_.id(1), suc.id()) and \
		   self.local_.id(1) != suc.id() and \
		   x.ping():
			self.local_.finger_[0] = x
		# We notify our new successor about us
		self.local_.successor().notify(self.local_)
		# Keep calling us
		for i in self.data_.iterkeys():
			if self.backup_.has_key(i):
				self.backup_.pop(i)
		return True

	@repeat_and_sleep(UPDATE_SUCCESSORS_INT)
	@retry_on_socket_error(UPDATE_SUCCESSORS_RET)
	def update_successors(self):
		self.local_.log("update successor")
		suc = self.local_.successor()
		# if we are not alone in the ring, calculate
		if suc.id() != self.local_.id():
			successors = [suc]
			suc_list = suc.get_successors()
			if suc_list and len(suc_list):
				successors += suc_list
			# if everything worked, we update
			if successors!=self.local_.successors_:
				self.local_.successors_ = successors
		return True

	def shutdown(self):
		self.local_.shutdown()
		self.shutdown_ = True

	def _get(self, request):
		try:
			data = request
			# we have the key
			return json.dumps({'status':'ok', 'data':self.get(data)})
		except Exception:
			# key not present
			traceback.print_exc()
			return json.dumps({'status':'failed'})

	def _set(self, request):
		try:
			key = request.split(' ')[0]
			value = request.split(' ')[1]
			self.set(key, value)
			return json.dumps({'status':'ok'})
			# something is not working
		except Exception:
			traceback.print_exc()
			return json.dumps({'status':'failed'})

	def get(self, key):
		try:
			print self.local_.id()
			return self.data_[key]#['value']
		except Exception:
			# not in our range
			suc = self.local_.find_successor(hash(key))
			if self.local_.id() == suc.id():
				# it's us but we don't have it
				if self.backup_.has_key(key):
					self.data_[key] = self.backup_[key]
					self.backupFlag = True
					print "Got at %s" % self.local_.id()
					return self.data_[key]#['value']
				return None
			try:
				response = suc.command('get %s' % key)
				if not response:
					raise Exception
				value = json.loads(response)
				if value['status'] != 'ok':
					raise Exception
				return value['data']
			except Exception:
				return None

	@repeat_and_sleep(4)
	def sendbackup(self):
		if self.backupFlag==False:
			pass
		else:
			temp = self.local_.successor()
			if self.data_ != {}:
				for i in [0,1]:
					if temp.id() == self.local_.id():
						break
				#while temp != self.local_:
					response = temp.command('backup %s' % json.dumps({"id":i,"address":self.local_.address(),
																	  "data":self.data_}))
					temp = temp.successor()
				print "Done backing up"
		self.backupFlag = False
		return True

	def setactual(self, suc, key, value):
		suc.command('set %s' % json.dumps({'key':key,'value':value}))

	def set(self, key, value):
		# get successor for key
		self.data_[key]=value#{"value":value, 'backedUp':False}
		if key in self.backup_:
			del self.backup_[key]
		suc = self.local_.find_successor(hash(key))
		if self.local_.id() == suc.id():
			#its us
			#print "Stored at %s" % self.local_.id()
			self.backupFlag = True
			try:
				#self.data_[key]['backedUp']=True
				return True
			except Exception:
				print traceback.print_exc(Exception)
		return True

	def _backup(self, request):
		#print "Backing up: " + request + " at %s" % self.local_.id()
		try:
			data = json.loads(request)
			id = data['id']
			address = data['address']
			value = data['data']
			self.backup(id, address, value)
			return json.dumps({'status':'ok'})
			# something is not working
		except Exception:
			traceback.print_exc()
			return json.dumps({'status':'failed'})

	def backup(self, id, address, value):
		#self.backup_ = {key: val for key, val in self.backup_.items()
        #     if val['address'] != address}
		#self.backup_[id]={"address":address, "data":value}
		for key in value:
			self.backup_[key]=value[key]
		return True

	def printbackup(self):
		temp = ""
		for i in self.backup_:
			temp += '{"'+i.__str__+'":"'+str(self.backup_[i])+'"}'
		return temp

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
			suc = self.local_.find_successor(hash(key))
			if self.local_.id() != suc.id():
				try:
					suc.command("set "+key+" "+self.data_[key])
					# print "moved %s into %s" % (key, node.id())
					to_remove.append(key)
					#print "migrated"
				except socket.error:
					print "error migrating"
					# we'll migrate it next time
					pass
		# remove all the keys we do not own any more
		if to_remove.__len__()!=0:
			self.backupFlag = True
		for key in to_remove:
			if key not in self.backup_:
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

