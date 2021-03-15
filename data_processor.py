import socket
import requests
import csv
import timeit
import multiprocessing


PORT = 4135

def find_between(data, first, last):
	try:
		start = data.index(first) + len(first)
		end = data.index(last, start)
		return data[start:end]
	except ValueError:
		return data
	
#reigister 

def reigister():
	try:
		p="http://localhost:8080/SetOutput?region=1&feedtype=IMBALANCE&output="+str(PORT)+"&status=on"
		r= requests.get(p)
		print(r.status_code)
		#print(r)
		if r.status_code==200:
			return True
		else:
			return False

	except Exception as e:
		return False

def data_processor():

	UDP_IP = "localhost"
	UDP_PORT = PORT

	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	sock.bind((UDP_IP, UDP_PORT))

	i = 0
	print("data taking start")
	while True:
		data, addr = sock.recvfrom(1024)
		stream_data = str(data)

		time_ = find_between(row, "MarketTime=", ",")
		symbol = find_between(row, "Symbol=", ",")
		market = symbol[-2:]
		symbol = symbol[:-3]
		volume =  find_between(row, "Volume=", ",")
		source = find_between(row, "Source=", ",")
		side = find_between(row, "Side=", ",")
		ts = timestamp(time_)
		i+=1
		if i%10000==0:print("data:",i)

def timestamp(s):
	p = s.split(":")
	try:
		x = int(p[0])*60+int(p[1])
		return x
	except Exception as e:
		print("Timestamp conversion error:",e)
		return 0

def sequential_process_experiment():

	start_time = timeit.default_timer()
	df= []
	with open('imbalance2.csv') as csv_file:
		csv_reader = csv.reader(csv_file, delimiter=',')
		line_count = 1
		for row in csv_reader:
			row = row[0]
			time = find_between(row, "MarketTime=", ",")
			ts = timestamp(time)
			symbol = find_between(row, "Symbol=", ",")
			market = symbol[-2:]
			symbol = symbol[:-3]
			volume =  find_between(row, "Volume=", ",")
			source = find_between(row, "Source=", ",")
			side = find_between(row, "Side=", ",")
			df.append([time,ts,symbol,market,volume,source,side])
			line_count+=1
			if line_count%1000==0:
				print("speed:",1000//(timeit.default_timer() - start_time))
				start_time = timeit.default_timer()

	#print("--- %s seconds ---" % (timeit.default_timer() - start_time),len(df))


def dual_process_experiment_writer(pipes):
	
	start_time = timeit.default_timer()
	with open('imbalance2.csv') as csv_file:
		csv_reader = csv.reader(csv_file, delimiter=',')
		line_count = 1
		for row in csv_reader:
			row = row[0]

			pipes[line_count%8].put_nowait(row)
			line_count+=1

			if line_count%1000==0:
				print("speed:",1000//(timeit.default_timer() - start_time))
				start_time = timeit.default_timer()
			#print(line_count)

		for i in pipes:
			i.put("fin")

	print("send finish")
	#print("Writer: --- %s seconds ---" % (timeit.default_timer() - start_time))

def dual_process_experiment_processor(pipe1,pipe2):

	df= []
	start_time = timeit.default_timer()
	chekc = 0
	while True:
		row = pipe1.recv()
		chekc+=1
		#print(chekc)
		if row =="finished":
			for i in range(20):
				pipe2.put("fin")
			break
		else:
			pipe2.put(row)
			#print(row)
#
	print("finished",len(df))
	print("Processor: --- %s seconds ---" % (timeit.default_timer() - start_time))


def multi_processing(queue):
	df = []
	print("p start")
	while True:
		row = queue.get()
		#print(row)
		if row=="fin":
			print("task finished")
			break
		time = find_between(row, "MarketTime=", ",")
		ts = timestamp(time)
		symbol = find_between(row, "Symbol=", ",")
		market = symbol[-2:]
		symbol = symbol[:-3]
		volume =  find_between(row, "Volume=", ",")
		source = find_between(row, "Source=", ",")
		side = find_between(row, "Side=", ",")
		df.append([time,ts,symbol,market,volume,source,side])

	print("len",len(df),df[-2:])

if __name__ == '__main__':

	#sequential_process_experiment()
	#2475320

	start_time = timeit.default_timer()

	multiprocessing.freeze_support()
	#receive_pipe,request = multiprocessing.Pipe()

	d1 = multiprocessing.Queue()
	d2 = multiprocessing.Queue()
	d3 = multiprocessing.Queue()
	d4 = multiprocessing.Queue()
	d5 = multiprocessing.Queue()
	d6 = multiprocessing.Queue()
	d7 = multiprocessing.Queue()
	d8 = multiprocessing.Queue()
	# p = multiprocessing.Process(target=dual_process_experiment_writer, args=(receive_pipe,),daemon=True)
	# p.daemon=True
	# p.start()


	p1 = multiprocessing.Process(target=multi_processing, args=(d1,),daemon=True)
	p1.daemon=True
	p2 = multiprocessing.Process(target=multi_processing, args=(d2,),daemon=True)
	p2.daemon=True
	p3 = multiprocessing.Process(target=multi_processing, args=(d3,),daemon=True)
	p3.daemon=True
	p4 = multiprocessing.Process(target=multi_processing, args=(d4,),daemon=True)
	p4.daemon=True
	p5 = multiprocessing.Process(target=multi_processing, args=(d5,),daemon=True)
	p5.daemon=True
	p6 = multiprocessing.Process(target=multi_processing, args=(d6,),daemon=True)
	p6.daemon=True
	p7 = multiprocessing.Process(target=multi_processing, args=(d7,),daemon=True)
	p7.daemon=True
	p8 = multiprocessing.Process(target=multi_processing, args=(d8,),daemon=True)
	p8.daemon=True
	# p1.start()
	# p2.start()
	# p3.start()
	# p4.start()
	# p5.start()
	# p6.start()
	# p7.start()
	# p8.start()
	distribution = [d1,d2,d3,d4,d5,d6,d7,d8]
	dual_process_experiment_writer(distribution)
	
	p1.join()
	p2.join()
	p3.join()
	p4.join()
	p5.join()
	p6.join()
	p7.join()
	p8.join()

	print("Total: --- %s seconds ---" % (timeit.default_timer() - start_time))