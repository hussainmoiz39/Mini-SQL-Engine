import sqlparse
import itertools
from prettytable import *
import os,sys,re

class Query():
	def __init__(self):
		self.Tables = []		 #tables
		self.tablcnt = 0
		self.Conds = []          #conditions
		self.Cols = []           #columns

	def parser(self,queryline):
		#print 'entered parser()'
		myline = queryline.strip()
		if not re.match(r'^(?i)(select\ ).+(?i)(\ from\ ).+[;]$', myline):
			print 'Error in sql syntax. Check the syntax. '
			return 0
		cs = re.sub(r'^(?i)(select\ )(.+)(?i)(\ from\ ).+[;]$', r'\2' , myline).split(',')
		#print 'kl'
		for i in cs:
			self.Cols.append(i.strip())
		#print 'kl1'
		if re.search(r'(?i)(\ where)[\ ]*$', myline):
			print 'Where conditions are not provided.'
			return 0
		#print 'kl2'	
		if re.search(r'(?i)(\ where\ )', myline):
			tempTables = re.sub(r'^(?i)(select\ ).+(?i)(\ from\ )(.+)(?i)(\ where\ )(.+)[;]$',r'\3', myline).split(',')
			#print 'kl3'
			for i in tempTables:
				self.Tables.append(i.strip())
			
			#print 'kl4'
			myConds = re.sub(r'^(?i)(select\ ).+(?i)(\ from\ ).+(?i)(\ where\ )(.+)[;]$',r'\4', myline).strip()  	
			
			#print 'kl5'
			if re.search(r'(?i)(\ or\ )',myConds):
				#print 'kl6'
				myConds = re.sub(r'^(.+)(?i)(or)(.+)$', r'\1 or \3' , myConds).split('or')
				for i in myConds:
					self.Conds.append(i.strip())
				return 3
			#print 'kl7'
			if re.search(r'(?i)(\ and\ )', myConds):
				myConds = re.sub(r'^(.+)(?i)(and)(.+)$', r'\1 and \3' , myConds).split('and')
				#print 'kl8'
				for i in myConds:
					self.Conds.append(i.strip())
				return 4
			self.Conds.append(myConds.strip())
			return 2
		
		else:
			myTables = re.sub(r'(?i)(^select\ ).+(?i)(\ from\ )(.+)[;]', r'\3', myline).split(',')
			#print 'kl9'
			#print myTables
			for i in myTables:
				self.Tables.append(i.strip())
			return 1	

			
		
		


































