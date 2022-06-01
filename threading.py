from threading import Thread


class Hello(thread):
	
	def run(self):
		for i in range(5):
			print("Hello")


class Hi(thread):
	def run(self):
		for i in range(5):
			print("Hi")
		


t1=Hello()
t2=Hi()

t1.start()
t2.start()