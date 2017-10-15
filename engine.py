import sqlparse
import itertools
from prettytable import *
from table import *
from query import *
import os,sys
import numpy as np

METADATA = 'metadata.txt'

class SqlEngine():

	def __init__(self):
		self.Tables = {}
		self.table_name = {}
		self.tabs = []
		self.Tcnt = 0
		self.readMetafile()
		self.TableReader()
		self.Engine()

	def readMetafile(self):
		with open(METADATA) as f:
			metadata = f.readlines()
			i = 0
			l = len(metadata)
			while l > i:
				if metadata[i].strip() == '<begin_table>':
					newtable = Table()
					i = i + 1
					newtable.Name = metadata[i].strip()
					i = i + 1
					while metadata[i].strip() != '<end_table>':
						newtable.Cols[ metadata[i].strip() ] = []   # to store col elements
						newtable.Attr.append( metadata[i].strip() ) #store col name
						i = i + 1
					self.Tables[ newtable.Name ] = newtable
					self.tabs.append( newtable.Name )
				i = i + 1
	
			 
	def TableReader(self):
		#print 'entered readTables()'
		for tablename in self.Tables:     #tablename is name of any table stored by engine
			with open(tablename + '.csv' ) as f:
				for rowline in f:
					rowline = rowline.split(',')
					self.Tables[tablename].nofrows += 1
					i = 0
					for col in self.Tables[ tablename ].Attr:
						rowline[i] = int( rowline[i].strip() )
						self.Tables[ tablename ].Cols[col].append( rowline[i] )
						i = i + 1
					self.Tables[ tablename ].Rows.append( rowline )

	def Checker(self, a, sgn, b ):
		#print 'entered check()'
		if sgn == '<>':
			return a != b
		elif sgn == '>':
			return a > b
		elif sgn == '<':
			return a < b
		elif sgn == '=':
			return a == b
		elif sgn == '>=':
			return a >= b
		elif sgn == '<=':
			return a <= b



	def getCols(self, tablename):
		#print 'enteed getCols()'
		return self.Tables[ tablename ].Attr


	def getTables(self, tablename ):
		#print 'entered getTables()'
		return self.Tables[tablename].Rows
		


	def ColProcess(self, query):
		#print 'Entered ColProc()'
		reqcols = []
		if '*' in query.Cols:
			query.Cols = []
			for tablename in query.Tables:
				reqcols += [ tablename + '.' + i for i in self.Tables[ tablename ].Attr]

		else:
			for col in query.Cols:
				if re.match(r'.+\..+',col):
					reqcols.append( col )
				
				else:
					cnt = 0
					for tablename in query.Tables:
						if col in self.Tables[ tablename ].Cols:
							t = tablename
							reqcols.append(t + '.' + col)
							cnt = cnt + 1
					#print 'cnt is : ',cnt
					if cnt == 0:
						print 'Column "' + col + '" not found.'
						return -1
					if cnt > 1:
						print 'Same Column with name-: "' + col + '" is in 2 or more tables. '
						return -1
		return reqcols
		

	def RowProcess(self, query):
		#print 'Entered RowProc'
		reqcols = self.ColProcess(query)
		if reqcols == -1:
			return

		T = PrettyTable(reqcols)
		for i in self.idx:
			rows = []
			#print 'Moiz'
			for col in reqcols:
				tablename = re.sub(r'(.+)\.(.+)', r'\1', col)
				colname = re.sub(r'(.+)\.(.+)', r'\2', col)

				if tablename not in self.Tables:
					print 'Table "' + tablename + '" not found.'
					return
				if colname not in self.Tables[ tablename ].Attr:
					print 'Column "' + co + '" not found.'
					return
					
				cnt = 0
				for j in self.Tables[ tablename ].Attr:
					if j == colname:
						break;
					cnt = cnt + 1

				rows.append(self.outtable[i][ self.table_name[tablename]][cnt])
			T.add_row(rows)
		print T
	
	def getDistinct(self,query):
		#print 'Entered getDist()'
		l = len(query.Cols)
		if l > 1:
			print 'Distinct can be used with 1 Column only. '
			return
		T = PrettyTable(query.Cols)
		colname = re.sub(r'.+\((.+)\)',r'\1',query.Cols[0]).strip()
		cn = []
		colname = colname.strip()
		tablename = ''

		if re.match(r'.+\..+',colname):
			colname2 = re.sub(r'(.+)\.(.+)',r'\2',colname)
			tablename = re.sub(r'(.+)\.(.+)',r'\1',colname)
		else:
			colname2 = colname
			cnt = 0
			for i in query.Tables:
				if colname in self.Tables[i].Cols:
					tablename = i
					cnt = cnt + 1
			if cnt == 0:
				print 'Column "' + colname2 + '" not found. '
				return
			elif cnt > 1:
				print 'Same Column "' + colname2 + '" exists in 2 or more tables. '
				return
		
			
		if tablename not in self.Tables:
			print 'Table "' + tablename + '" not found.'
			return
		if colname2 not in self.Tables[tablename].Attr:
			print 'Column "' + colname2 + '" not found.'
			return

		cn = 0
		for j in self.Tables[tablename].Attr:
			if j == colname2:
				break;
			cn += 1
		distinct = {}
		for i in self.idx:
			rows = []
			tupl = self.outtable[i][self.table_name[tablename]][cn]
			if tupl not in distinct:
				rows.append(tupl)
				distinct[tupl] = 1
			if rows:
				T.add_row(rows)
		print T

		
	
	def getAggregate(self,query):
		#print 'Entered getAgg()'
		rows = []
		T = PrettyTable(query.Cols)
		
		for colname in query.Cols:
			f = re.sub(r'\(.+\)','',colname) #function
			a = re.sub(r'.+\((.+)\)',r'\1',colname).strip() #attribute
			tablename = ''
			if re.match(r'.+\..+',a):
				col = re.sub(r'(.+)\.(.+)',r'\2',a)
				tablename = re.sub(r'(.+)\.(.+)',r'\1',a)
			
			else:
				col = a
				cnt = 0
				for i in query.Tables:   #iterate thr table-names stored in Tables
					if a in self.Tables[i].Cols:
						tablename = i
						cnt += 1
						
				if cnt == 0:
					print 'Column "' + col + 'not found.'
					return
				elif cnt > 1:
					print 'Same Column in 2 tables.'
					return
			if tablename not in self.Tables:
				print 'Table "' + tablename + '" not found.'
				return
			
			if col not in self.Tables[tablename].Attr:
				print 'Column "' + col + 'not found.'
				return
			
			cnt = 0
			for j in self.Tables[tablename].Attr:
				if j == col:
					break;
				cnt += 1	
			
			if re.match(r'(?i)(avg)',f):
				res = 0
				for i in self.idx:
					res += self.outtable[i][self.table_name[tablename]][cnt]
				k = len(self.idx)
				res /= k
			elif re.match(r'(?i)(sum)',f):
				res = 0

				for i in self.idx:
					res += self.outtable[i][self.table_name[tablename]][cnt]
			elif re.match(r'(?i)(max)',f):
				res = -sys.maxint - 5
				for i in self.idx:
					res = max(res, self.outtable[i][self.table_name[tablename]][cnt])
			elif re.match(r'(?i)(min)',f):
				res = sys.maxint
				for i in self.idx:
					res = min(res, self.outtable[i][self.table_name[tablename]][cnt])
						
			rows.append(res)
		T.add_row(rows)
		print T

	
	

	def parseCond(self,query,idx):
		#print 'Entered parseCond()'
		result = []

		if not re.match(r'([^<>=]+)(<|=|>|<>|<=|>=)([^<>=]+)',query.Conds[idx]):
			print 'Condition is Invalid'
			return -1

		a = re.sub(r'(.+)(<|=|>|<>|<=|>=)(.+)',r'\1',query.Conds[idx]).strip()
		op = re.sub(r'(.+)(<|=|>|<>|<=|>=)(.+)',r'\2',query.Conds[idx]).strip()
		b = re.sub(r'(.+)(<|=|>|<>|<=|>=)(.+)',r'\3',query.Conds[idx]).strip()

		#print 'len a is: ',len(a)
		tmp = a
		tmp = tmp.split(' ')
		#print tmp,len(tmp)

		#k = len(tmp)
		#tmp2 = list(tmp)
		#print tmp2
		#print'len of tmp2: ',len(tmp2)
		#print 'hello ',tmp[k-1] 
		#if tmp[k-1] == '<' or tmp[k-1] == '>':
		#	a = tmp[0,k-2]
		#	op = tmp[k-1] + op
		
		if len(tmp) == 2:      #for >=,<=
			a = tmp[0]
			op = tmp[1] + op

		#print 'a: ',a
		#print 'op: ',op
		#print 'b: ',b


		val = 0

		try:
			b = int(b)
			val = 1
		except ValueError:
			val = 0


		tablename = ''
		if re.match(r'(.+)\.(.+)',a):
			tablename = re.sub(r'(.+)\.(.+)',r'\1',a)
			col = re.sub(r'(.+)\.(.+)',r'\2',a)
		
		else:
			cnt = 0
			col = a
			for i in query.Tables:
				if col in self.Tables[i].Cols:
					tablename = i
					cnt = cnt + 1
			
			if cnt == 0:
				print 'Column ' + col + ' not found. '
				return -1
			elif cnt > 1:
				print 'Column "' + co + '" exists in 2 or more tables.'
				return -1
		
		if tablename not in self.Tables:
			print 'Table ' + tablename + ' not found. '
			return -1
		
		if col not in self.Tables[tablename].Attr:
			print 'Column ' + col + ' is not found. '
			return -1
		
		if val:
			cn = 0
			for i in self.Tables[tablename].Attr:
				if i == col:
					break
				cn = cn + 1
			n = 1
			for i in query.Tables:
				n *= self.Tables[i].nofrows
			for i in xrange(n):
				if self.Checker(self.outtable[i][self.table_name[tablename]][cn], op, b):
					result.append(i)
		
		else:
			tableL = tablename
			tableR = ''
			Lcol = col
			if re.match(r'(.+)\.(.+)',b):
				tableR = re.sub(r'(.+)\.(.+)',r'\1',b)
				Rcol = re.sub(r'(.+)\.(.+)',r'\2',b)
			else:
				Rcol = b
				cnt = 0
				for i in query.Tables:
					if Rcol in self.Tables[tablename].Cols:
						tableR = i
						cnt = cnt + 1
				
				if cnt == 0:
					print 'Column "' + Rcol + '" not found.'
					return -1
				if cnt > 1:
					print 'Same Column "' + Rcol + '" exists in 2 or more tables.'
					return -1
			
			if tableR not in self.Tables:
				print 'Table "' + tableR + '" not found.'
				return -1
			
			if Rcol not in self.Tables[tableR].Attr:
				print 'Column "' + rco + '" not found.'
				return -1

				
			cntl = 0
			cntr = 0

			for i in self.Tables[tableL].Attr:
				if i == Lcol:
					break;
				cntl = cntl + 1
			
			for i in self.Tables[tableR].Attr:
				if i == Rcol:
					break;
				cntr = cntr + 1
			
			n = 1

			for i in query.Tables:
				n *= self.Tables[i].nofrows
			
			for i in xrange(n):
				if self.Checker(self.outtable[i][self.table_name[tableL]][cntl],op,self.outtable[i][self.table_name[tableR]][cntr]):
					result.append(i)

		return result
					




	def parse(self,query,flg):
		#print 'Entered parse()'
		#print 'flg is',flg
		if flg == 1:
			n = 1
			for i in query.Tables:

				n *= self.Tables[i].nofrows
			#print 'n is : ',n
			self.idx = xrange(n)
		
		elif flg == 2:
			res = self.parseCond(query,0)
			if res == -1:
				return
			else:
				self.idx = res

		else:
			res1 = self.parseCond(query,0)
			res2 = self.parseCond(query,1)
			
			if res1 == -1 or res2 == -1:
				return
			elif flg == 3:
				self.idx = list(set(res1) | set(res2) )
			elif flg == 4:
				self.idx = list(set(res1) & set(res2) )

		if any(re.match(r'(?i)(distinct)', x) for x in query.Cols):
			self.getDistinct(query)
			return
		
		for x in query.Cols:
			if re.match(r'.+\(.+\)',x):
				self.getAggregate(query)
				return
		self.RowProcess(query)






			
	def Engine(self):
		#print 'Entered engine()'
		engflg = 1
		while engflg:
			

			try:
				inpt = raw_input('mysql>')   #inpt->input
				
				if inpt == 'quit' or inpt == 'exit' or inpt == 'quit;' or inpt == 'exit;':
					engflg = 0
					exit(0)
					
				else:	
					queries = inpt.split(';')
					noq = len(queries)          #no of queries
					f = np.ones(noq)
				
					if queries[noq-1] != '':
						f[noq-1] = 0;
				
					for i in xrange(noq):
						self.outcols = []
						self.outtable = []
						if not queries[i]:
							continue
						query = Query()
						if f[i]:
							queries[i] = queries[i] + ';'
						#print queries[i]	
						flg = query.parser(queries[i])
						if not flg:
							continue
						flg2 = 0
						cnt = 0
						for j in query.Tables:

					 		if j not in self.Tables:
					 			print 'Table "' + j +'" not found. '
					 			flg2 = 1
					 			break
					 		self.table_name[j] = cnt
					 		cnt = cnt + 1
					
						if flg2:
							continue

						for j in itertools.product(*map(self.getTables,query.Tables)):
							self.outtable.append(j)
					
						self.outcols = map(self.getCols,query.Tables)
						self.idx = []
						self.parse(query,flg)
					
						
			except Exception as e:
				print e
					


	



		




































